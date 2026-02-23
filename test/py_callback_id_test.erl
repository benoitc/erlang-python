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

-module(py_callback_id_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Test fixtures
%% ============================================================================

setup() ->
    py_callback_id:init().

cleanup(_) ->
    ok.

%% ============================================================================
%% Tests
%% ============================================================================

callback_id_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"next returns positive integer", fun test_next_positive/0},
      {"next is monotonically increasing", fun test_monotonic/0},
      {"concurrent calls produce unique IDs", fun test_concurrent/0}
     ]}.

test_next_positive() ->
    Id = py_callback_id:next(),
    ?assert(is_integer(Id)),
    ?assert(Id > 0).

test_monotonic() ->
    Id1 = py_callback_id:next(),
    Id2 = py_callback_id:next(),
    Id3 = py_callback_id:next(),
    ?assert(Id2 > Id1),
    ?assert(Id3 > Id2).

test_concurrent() ->
    Self = self(),
    NumProcesses = 100,
    IdsPerProcess = 100,

    %% Spawn processes that each generate IDs
    Pids = [spawn_link(fun() ->
        Ids = [py_callback_id:next() || _ <- lists:seq(1, IdsPerProcess)],
        Self ! {ids, self(), Ids}
    end) || _ <- lists:seq(1, NumProcesses)],

    %% Collect all IDs
    AllIds = lists:flatmap(fun(Pid) ->
        receive
            {ids, Pid, Ids} -> Ids
        after 5000 ->
            error({timeout, Pid})
        end
    end, Pids),

    %% Verify all IDs are unique
    UniqueIds = lists:usort(AllIds),
    ?assertEqual(length(AllIds), length(UniqueIds)),

    %% Verify we got the expected number
    ?assertEqual(NumProcesses * IdsPerProcess, length(AllIds)).
