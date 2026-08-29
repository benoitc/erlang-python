%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc Shared memory regions between Erlang and Python contexts.
%%%
%%% A region is a fixed-size file mapped `MAP_SHARED' through
%%% <a href="https://hex.pm/packages/iommap">iommap</a>. Erlang reads it
%%% with no copy (`binary/3') and writes with one copy (`write/3'); Python
%%% maps the same file and sees it as a buffer (`erlang.SharedMemory'),
%%% in every context mode. The handle is a plain term,
%%% `{'$py_shm', Id, Path, Size}', so it travels inside any call argument or
%%% result.
%%%
%%% ```
%%% {ok, Shm} = py_shm:new(64 * 1024 * 1024),
%%% ok = py_shm:write(Shm, 0, Data),
%%% {ok, Sum} = py_context:call(Ctx, myapp, sum_floats, [Shm]),
%%% Out = py_shm:binary(Shm, 0, 1024),
%%% ok = py_shm:close(Shm).
%%% '''
%%%
%%% iommap is an optional dependency: add `{iommap, "1.1.3"}' to your deps.
%%% Without it `new/1,2' returns `{error, iommap_not_available}'.
%%%
%%% The module also backs shared `py_buffer's (`py_buffer:new(#{shared => true})'):
%%% a region used as a ring, with the write position and the closed flag in
%%% a header page and flow control through the `_py_buffer_wait' and
%%% `_py_buffer_consumed' callbacks the Python side calls.
-module(py_shm).

-behaviour(gen_server).

-export([
    start_link/0,
    available/0,
    new/1,
    new/2,
    read_only/1,
    write/3,
    read/3,
    binary/3,
    size/1,
    close/1,
    info/1,
    %% Shared buffers (used by py_buffer)
    buffer_new/1,
    buffer_write/2,
    buffer_write/3,
    buffer_close/1,
    buffer_info/1,
    %% Callbacks the Python side uses
    register_callbacks/0,
    handle_buffer_wait/1,
    handle_buffer_consumed/1,
    handle_buffer_state/1,
    %% Location of region files (also used by isolated contexts)
    private_dir/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TABLE, py_shm_regions).
-define(HEADER, 4096).
-define(DEFAULT_RING, 4 * 1024 * 1024).
-define(DEFAULT_WRITE_TIMEOUT, 30000).
-define(IS_SHM(T), (T =:= '$py_shm' orelse T =:= '$py_shm_ro')).

-type shm() :: {'$py_shm' | '$py_shm_ro', pos_integer(), binary(), non_neg_integer()}.
-type buffer() :: {'$py_buffer', pos_integer(), binary(), pos_integer()}.
-export_type([shm/0, buffer/0]).

%% Ring buffer state
-record(buf, {
    id :: pos_integer(),
    handle :: term(),
    ring :: pos_integer(),
    wpos = 0 :: non_neg_integer(),      %% total bytes written
    rpos = 0 :: non_neg_integer(),      %% total bytes consumed
    closed = false :: boolean(),
    readers = [] :: [{gen_server:from(), non_neg_integer()}],
    %% Writers waiting for room: {From, Rest, TimerRef}
    writers = [] :: [{gen_server:from(), binary(), reference()}]
}).

-record(state, {
    buffers = #{} :: #{pos_integer() => #buf{}}
}).

%% ============================================================================
%% API
%% ============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Whether iommap is available: shared memory needs it.
-spec available() -> boolean().
available() ->
    code:ensure_loaded(iommap) =:= {module, iommap}.

%% @doc Create a region of `Size' bytes owned by the calling process.
-spec new(pos_integer()) -> {ok, shm()} | {error, term()}.
new(Size) ->
    new(Size, #{}).

%% @doc Create a region. Options: `owner' (pid whose exit closes the region,
%% default the caller); `writable' (default `true'): with `false' Python maps
%% the region read-only, so a buggy or hostile callee cannot change it.
-spec new(pos_integer(), map()) -> {ok, shm()} | {error, term()}.
new(Size, Opts) when is_integer(Size), Size > 0, is_map(Opts) ->
    Owner = maps:get(owner, Opts, self()),
    case gen_server:call(?MODULE, {new, Size, Owner}, infinity) of
        {ok, {'$py_shm', Id, Path, Size}} when map_get(writable, Opts) =:= false ->
            {ok, {'$py_shm_ro', Id, Path, Size}};
        Other ->
            Other
    end.

%% @doc Read-only view of a handle for the Python side; Erlang keeps writing.
-spec read_only(shm()) -> shm().
read_only({Tag, Id, Path, Size}) when ?IS_SHM(Tag) ->
    {'$py_shm_ro', Id, Path, Size}.

%% @doc Copy `Data' into the region at `Offset'.
-spec write(shm(), non_neg_integer(), binary()) -> ok | {error, term()}.
write({Tag, Id, _, Size}, Offset, Data) when ?IS_SHM(Tag), is_binary(Data) ->
    case Offset + byte_size(Data) > Size of
        true -> {error, out_of_bounds};
        false ->
            case lookup(Id) of
                {ok, Handle} -> iommap(pwrite, [Handle, Offset, Data]);
                error -> {error, closed}
            end
    end.

%% @doc Copy `Len' bytes out of the region.
-spec read(shm(), non_neg_integer(), non_neg_integer()) -> {ok, binary()} | {error, term()}.
read({Tag, Id, _, Size}, Offset, Len) when ?IS_SHM(Tag) ->
    case Offset + Len > Size of
        true -> {error, out_of_bounds};
        false ->
            case lookup(Id) of
                {ok, Handle} -> iommap(pread, [Handle, Offset, Len]);
                error -> {error, closed}
            end
    end.

%% @doc A binary over the region, no copy. It stays valid after `close/1'
%% (the mapping is kept as long as the binary is referenced). Its bytes are
%% the region's: they change when Python writes, so treat it as a snapshot
%% only once the callee is done with the handle, or use `read/3' for a copy.
-spec binary(shm(), non_neg_integer(), non_neg_integer()) -> binary().
binary({Tag, Id, _, Size}, Offset, Len) when ?IS_SHM(Tag) ->
    Offset + Len =< Size orelse error(out_of_bounds),
    case lookup(Id) of
        {ok, Handle} ->
            case iommap(region_binary, [Handle, Offset, Len]) of
                {ok, Bin} -> Bin;
                {error, Reason} -> error(Reason)
            end;
        error ->
            error(closed)
    end.

-spec size(shm()) -> non_neg_integer().
size({Tag, _, _, Size}) when ?IS_SHM(Tag) ->
    Size.

%% @doc Close the region: the file is removed and the iommap handle closed.
%% Python mappings stay valid until the wrapper is closed or collected.
-spec close(shm() | buffer()) -> ok.
close({Tag, Id, _, _}) when ?IS_SHM(Tag) ->
    gen_server:call(?MODULE, {close, Id}, infinity);
close({'$py_buffer', _, _, _} = Buf) ->
    buffer_close(Buf).

-spec info(shm()) -> {ok, map()} | {error, term()}.
info({Tag, Id, Path, Size}) when ?IS_SHM(Tag) ->
    case lookup(Id) of
        {ok, _} -> {ok, #{id => Id, path => Path, size => Size}};
        error -> {error, closed}
    end.

%% ---- shared buffers --------------------------------------------------------

%% @doc Create a shared streaming buffer (ring of `RingSize' bytes).
-spec buffer_new(map()) -> {ok, buffer()} | {error, term()}.
buffer_new(Opts) when is_map(Opts) ->
    Ring = maps:get(size, Opts, ?DEFAULT_RING),
    Owner = maps:get(owner, Opts, self()),
    gen_server:call(?MODULE, {buffer_new, Ring, Owner}, infinity).

-spec buffer_write(buffer(), binary()) -> ok | {error, term()}.
buffer_write(Buf, Data) ->
    buffer_write(Buf, Data, ?DEFAULT_WRITE_TIMEOUT).

%% @doc Append `Data'; blocks up to `Timeout' ms while the ring is full.
-spec buffer_write(buffer(), binary(), timeout()) -> ok | {error, term()}.
buffer_write({'$py_buffer', Id, _, _}, Data, Timeout) when is_binary(Data) ->
    gen_server:call(?MODULE, {buffer_write, Id, Data, Timeout}, infinity).

-spec buffer_close(buffer()) -> ok.
buffer_close({'$py_buffer', Id, _, _}) ->
    gen_server:call(?MODULE, {buffer_close, Id}, infinity).

-spec buffer_info(buffer()) -> {ok, map()} | {error, term()}.
buffer_info({'$py_buffer', Id, _, _}) ->
    gen_server:call(?MODULE, {buffer_info, Id}, infinity).

%% ---- callbacks used from Python --------------------------------------------

%% @private Registered by the supervisor once py_callback is up.
register_callbacks() ->
    py_callback:register(<<"_py_buffer_wait">>, {?MODULE, handle_buffer_wait}),
    py_callback:register(<<"_py_buffer_consumed">>, {?MODULE, handle_buffer_consumed}),
    py_callback:register(<<"_py_buffer_state">>, {?MODULE, handle_buffer_state}),
    ok.

%% @private Block until the write position passes `ReadPos' or the buffer is
%% closed. Returns `{WPos, Closed}'.
handle_buffer_wait([Id, ReadPos]) ->
    gen_server:call(?MODULE, {buffer_wait, Id, ReadPos}, infinity).

%% @private The reader consumed `N' bytes: make room for writers.
handle_buffer_consumed([Id, N]) ->
    gen_server:call(?MODULE, {buffer_consumed, Id, N}, infinity).

%% @private Current `{WPos, Closed}' without waiting.
handle_buffer_state([Id]) ->
    gen_server:call(?MODULE, {buffer_state, Id}, infinity).

%% @doc Private directory for region files: `/dev/shm' when it exists
%% (memory backed), else a 0700 directory under `TMPDIR'.
-spec private_dir() -> string().
private_dir() ->
    Base = case filelib:is_dir("/dev/shm") of
        true -> "/dev/shm";
        false ->
            case os:getenv("TMPDIR") of
                false -> "/tmp";
                T -> T
            end
    end,
    Dir = filename:join(Base, "erlang_python_" ++ os:getpid()),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    _ = file:change_mode(Dir, 8#700),
    Dir.

%% ============================================================================
%% gen_server
%% ============================================================================

init([]) ->
    ?TABLE = ets:new(?TABLE, [named_table, protected, set, {read_concurrency, true}]),
    {ok, #state{}}.

handle_call({new, Size, Owner}, _From, State) ->
    {reply, create_region(Size, Owner), State};

handle_call({close, Id}, _From, State) ->
    close_region(Id),
    {reply, ok, State};

handle_call({buffer_new, Ring, Owner}, _From, #state{buffers = Bufs} = State) ->
    case create_region(?HEADER + Ring, Owner) of
        {ok, {'$py_shm', Id, Path, _}} ->
            {ok, Handle} = lookup(Id),
            Buf = #buf{id = Id, handle = Handle, ring = Ring},
            ok = write_header(Buf),
            {reply, {ok, {'$py_buffer', Id, Path, Ring}}, State#state{buffers = Bufs#{Id => Buf}}};
        {error, _} = Err ->
            {reply, Err, State}
    end;

handle_call({buffer_write, Id, Data, Timeout}, From, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := #buf{closed = true}} ->
            {reply, {error, closed}, State};
        #{Id := Buf} ->
            case do_write(Buf, Data) of
                {ok, Buf1} ->
                    {reply, ok, State#state{buffers = Bufs#{Id => Buf1}}};
                {partial, Buf1, Rest} ->
                    Timer = erlang:send_after(Timeout, self(), {write_timeout, Id, From}),
                    Buf2 = Buf1#buf{writers = Buf1#buf.writers ++ [{From, Rest, Timer}]},
                    {noreply, State#state{buffers = Bufs#{Id => Buf2}}}
            end;
        _ ->
            {reply, {error, closed}, State}
    end;

handle_call({buffer_close, Id}, _From, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := Buf} ->
            Buf1 = Buf#buf{closed = true},
            ok = write_header(Buf1),
            %% Readers learn about EOF; writers still waiting fail
            [gen_server:reply(R, {Buf1#buf.wpos, true}) || {R, _} <- Buf1#buf.readers],
            [begin erlang:cancel_timer(T), gen_server:reply(W, {error, closed}) end
             || {W, _, T} <- Buf1#buf.writers],
            {reply, ok, State#state{buffers = Bufs#{Id => Buf1#buf{readers = [], writers = []}}}};
        _ ->
            {reply, ok, State}
    end;

handle_call({buffer_wait, Id, ReadPos}, From, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := #buf{wpos = W, closed = C}} when W > ReadPos; C ->
            {reply, {W, C}, State};
        #{Id := Buf} ->
            Buf1 = Buf#buf{readers = [{From, ReadPos} | Buf#buf.readers]},
            {noreply, State#state{buffers = Bufs#{Id => Buf1}}};
        _ ->
            {reply, {error, closed}, State}
    end;

handle_call({buffer_consumed, Id, N}, _From, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := Buf} ->
            Buf1 = Buf#buf{rpos = Buf#buf.rpos + N},
            Buf2 = drain_writers(Buf1),
            {reply, ok, State#state{buffers = Bufs#{Id => Buf2}}};
        _ ->
            {reply, {error, closed}, State}
    end;

handle_call({buffer_state, Id}, _From, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := #buf{wpos = W, closed = C}} -> {reply, {W, C}, State};
        _ -> {reply, {error, closed}, State}
    end;

handle_call({buffer_info, Id}, _From, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := #buf{wpos = W, rpos = R, closed = C, ring = Ring}} ->
            {reply, {ok, #{written => W, consumed => R, closed => C, ring => Ring,
                           pending_writers => length((maps:get(Id, Bufs))#buf.writers)}}, State};
        _ ->
            {reply, {error, closed}, State}
    end;

handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({write_timeout, Id, From}, #state{buffers = Bufs} = State) ->
    case Bufs of
        #{Id := #buf{writers = Ws} = Buf} ->
            case lists:keytake(From, 1, Ws) of
                {value, {From, _Rest, _T}, Rest} ->
                    gen_server:reply(From, {error, timeout}),
                    {noreply, State#state{buffers = Bufs#{Id => Buf#buf{writers = Rest}}}};
                false ->
                    {noreply, State}
            end;
        _ ->
            {noreply, State}
    end;
handle_info({'DOWN', _Mon, process, Owner, _Reason}, #state{buffers = Bufs} = State) ->
    Ids = [Id || {Id, _, _, _, O} <- ets:tab2list(?TABLE), O =:= Owner],
    [close_region(Id) || Id <- Ids],
    Bufs1 = maps:without(Ids, Bufs),
    {noreply, State#state{buffers = Bufs1}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [close_region(Id) || {Id, _, _, _, _} <- ets:tab2list(?TABLE)],
    ok.

%% ============================================================================
%% Internal
%% ============================================================================

%% iommap is an optional dependency: call it indirectly so xref and
%% dialyzer do not require it in the default profile.
iommap(Fun, Args) ->
    apply(iommap, Fun, Args).

lookup(Id) ->
    try ets:lookup(?TABLE, Id) of
        [{Id, Handle, _Path, _Size, _Owner}] -> {ok, Handle};
        [] -> error
    catch
        error:badarg -> error
    end.

create_region(Size, Owner) ->
    case available() of
        false ->
            {error, iommap_not_available};
        true ->
            Id = erlang:unique_integer([positive]),
            Path = filename:join(private_dir(), "shm_" ++ integer_to_list(Id)),
            case iommap(open, [Path, read_write, [create, truncate, {size, Size}, shared]]) of
                {ok, Handle} ->
                    _ = file:change_mode(Path, 8#600),
                    _ = erlang:monitor(process, Owner),
                    ets:insert(?TABLE, {Id, Handle, Path, Size, Owner}),
                    {ok, {'$py_shm', Id, unicode:characters_to_binary(Path), Size}};
                {error, Reason} ->
                    {error, {shm_open_failed, Reason}}
            end
    end.

close_region(Id) ->
    case ets:take(?TABLE, Id) of
        [{Id, Handle, Path, _Size, _Owner}] ->
            _ = file:delete(Path),
            _ = iommap(close, [Handle]),
            ok;
        [] ->
            ok
    end.

%% Header page: <<WPos:64/native, Closed:8, Ring:64/native>>
write_header(#buf{handle = H, wpos = W, closed = C, ring = Ring}) ->
    Flag = case C of true -> 1; false -> 0 end,
    iommap(pwrite, [H, 0, <<W:64/native, Flag:8, Ring:64/native>>]).

%% Copy as much of Data as fits, advance wpos, wake readers.
do_write(#buf{ring = Ring, wpos = W, rpos = R} = Buf, Data) ->
    Free = Ring - (W - R),
    Size = byte_size(Data),
    Take = min(Free, Size),
    Buf1 = case Take > 0 of
        true ->
            <<Chunk:Take/binary, _/binary>> = Data,
            ok = ring_write(Buf, W, Chunk),
            B = Buf#buf{wpos = W + Take},
            ok = write_header(B),
            wake_readers(B);
        false ->
            Buf
    end,
    case Take =:= Size of
        true -> {ok, Buf1};
        false ->
            <<_:Take/binary, Rest/binary>> = Data,
            {partial, Buf1, Rest}
    end.

ring_write(#buf{handle = H, ring = Ring}, Pos, Chunk) ->
    Off = Pos rem Ring,
    Size = byte_size(Chunk),
    case Off + Size =< Ring of
        true ->
            iommap(pwrite, [H, ?HEADER + Off, Chunk]);
        false ->
            First = Ring - Off,
            <<A:First/binary, B/binary>> = Chunk,
            ok = iommap(pwrite, [H, ?HEADER + Off, A]),
            iommap(pwrite, [H, ?HEADER, B])
    end.

wake_readers(#buf{readers = Readers, wpos = W, closed = C} = Buf) ->
    {Ready, Waiting} = lists:partition(fun({_, Pos}) -> W > Pos end, Readers),
    [gen_server:reply(From, {W, C}) || {From, _} <- Ready],
    Buf#buf{readers = Waiting}.

%% Room was made: continue pending writers in order.
drain_writers(#buf{writers = []} = Buf) ->
    Buf;
drain_writers(#buf{writers = [{From, Rest, Timer} | Others]} = Buf) ->
    case do_write(Buf#buf{writers = Others}, Rest) of
        {ok, Buf1} ->
            erlang:cancel_timer(Timer),
            gen_server:reply(From, ok),
            drain_writers(Buf1);
        {partial, Buf1, Rest1} ->
            Buf1#buf{writers = [{From, Rest1, Timer} | Others]}
    end.
