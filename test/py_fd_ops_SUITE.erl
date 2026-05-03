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

%%% @doc Test suite for direct FD operations (fd_read, fd_write, fd_close, socketpair).
-module(py_fd_ops_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    socketpair_test/1,
    fd_read_write_test/1,
    fd_close_test/1,
    fd_select_test/1,
    dup_fd_test/1
]).

all() ->
    [
        socketpair_test,
        fd_read_write_test,
        fd_close_test,
        fd_select_test,
        dup_fd_test
    ].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Test socketpair creation
socketpair_test(_Config) ->
    {ok, {Fd1, Fd2}} = py_nif:socketpair(),
    true = is_integer(Fd1),
    true = is_integer(Fd2),
    true = Fd1 >= 0,
    true = Fd2 >= 0,
    true = Fd1 =/= Fd2,
    %% Clean up
    ok = py_nif:fd_close(Fd1),
    ok = py_nif:fd_close(Fd2),
    ok.

%% @doc Test fd_read and fd_write
fd_read_write_test(_Config) ->
    {ok, {Fd1, Fd2}} = py_nif:socketpair(),

    %% Write to Fd1, read from Fd2
    TestData = <<"hello world">>,
    {ok, Written} = py_nif:fd_write(Fd1, TestData),
    Written = byte_size(TestData),

    %% Small delay to ensure data is available
    timer:sleep(10),

    {ok, ReadData} = py_nif:fd_read(Fd2, 1024),
    TestData = ReadData,

    %% Write to Fd2, read from Fd1
    TestData2 = <<"response data">>,
    {ok, Written2} = py_nif:fd_write(Fd2, TestData2),
    Written2 = byte_size(TestData2),

    timer:sleep(10),

    {ok, ReadData2} = py_nif:fd_read(Fd1, 1024),
    TestData2 = ReadData2,

    %% Clean up
    ok = py_nif:fd_close(Fd1),
    ok = py_nif:fd_close(Fd2),
    ok.

%% @doc Test fd_close
fd_close_test(_Config) ->
    {ok, {Fd1, Fd2}} = py_nif:socketpair(),

    %% Close should succeed
    ok = py_nif:fd_close(Fd1),
    ok = py_nif:fd_close(Fd2),

    %% Reading from closed fd should fail
    {error, _} = py_nif:fd_read(Fd1, 1024),
    ok.

%% @doc Test fd_select_read and fd_select_write
fd_select_test(_Config) ->
    {ok, {Fd1, Fd2}} = py_nif:socketpair(),

    %% Register for read on Fd2 - returns {ok, FdRef}
    {ok, ReadRef} = py_nif:fd_select_read(Fd2),
    true = is_reference(ReadRef),

    %% Write data to Fd1 (should trigger read ready on Fd2)
    TestData = <<"select test">>,
    {ok, _} = py_nif:fd_write(Fd1, TestData),

    %% Should receive select message with the FdRef
    receive
        {select, ReadRef, _Ref, ready_input} ->
            ok
    after 1000 ->
        ct:fail(select_timeout)
    end,

    %% Read the data
    {ok, ReadData} = py_nif:fd_read(Fd2, 1024),
    TestData = ReadData,

    %% Test write select - sockets are usually immediately writable
    {ok, WriteRef} = py_nif:fd_select_write(Fd1),
    true = is_reference(WriteRef),
    receive
        {select, WriteRef, _Ref2, ready_output} ->
            ok
    after 1000 ->
        ct:fail(write_select_timeout)
    end,

    %% Clean up
    ok = py_nif:fd_close(Fd1),
    ok = py_nif:fd_close(Fd2),
    ok.

%% @doc Test py:dup_fd/1 — duplicates an fd so caller and dup can be
%% closed independently and both refer to the same kernel descriptor.
dup_fd_test(_Config) ->
    {ok, {Fd1, Fd2}} = py_nif:socketpair(),

    %% Duplicate Fd2; the dup must be a different integer.
    {ok, DupFd2} = py:dup_fd(Fd2),
    true = is_integer(DupFd2),
    true = DupFd2 =/= Fd2,
    true = DupFd2 >= 0,

    %% Write through Fd1 — both Fd2 and DupFd2 see the same data
    %% because they reference the same socket end.
    Payload = <<"dup_fd round-trip">>,
    {ok, _} = py_nif:fd_write(Fd1, Payload),
    timer:sleep(10),
    {ok, ReadFromOriginal} = py_nif:fd_read(Fd2, 1024),
    Payload = ReadFromOriginal,

    %% Close the original fd. The duplicate must remain usable.
    ok = py_nif:fd_close(Fd2),
    Payload2 = <<"after-close">>,
    {ok, _} = py_nif:fd_write(Fd1, Payload2),
    timer:sleep(10),
    {ok, ReadFromDup} = py_nif:fd_read(DupFd2, 1024),
    Payload2 = ReadFromDup,

    %% Clean up
    ok = py_nif:fd_close(DupFd2),
    ok = py_nif:fd_close(Fd1),
    ok.
