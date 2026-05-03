%%% @doc Regression contract for context thread affinity with real
%%% ML libraries.
%%%
%%% v3.0 fixed numpy / torch / tensorflow segfaults caused by the
%%% executor pool moving calls across OS threads. The fix is per-context
%%% worker pthreads with stable thread affinity. `py_thread_affinity_SUITE'
%%% checks the threading.get_native_id invariants in isolation;
%%% this suite drives actual numpy and tensorflow operations through
%%% exec / eval / call paths to confirm the libraries' thread-local
%%% state survives the round-trip.
%%%
%%% Skip-on-missing for both libraries: cases that need an unavailable
%%% module return {skip, ...} from init_per_testcase. TensorFlow is
%%% always skipped on CI (too heavy to install). Owngil cases additionally
%%% skip on Python <3.14.
-module(py_ml_libs_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    numpy_basic_ops/1,
    numpy_call_thread_affinity/1,
    numpy_parallel_processes/1,
    numpy_owngil_basic/1,
    tensorflow_basic_ops/1,
    tensorflow_call_thread_affinity/1
]).

all() ->
    [
        numpy_basic_ops,
        numpy_call_thread_affinity,
        numpy_parallel_processes,
        numpy_owngil_basic,
        tensorflow_basic_ops,
        tensorflow_call_thread_affinity
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    %% Suppress TensorFlow's chatty C++ logging at import time.
    %% Must be set before TF is imported in any context.
    os:putenv("TF_CPP_MIN_LOG_LEVEL", "3"),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(TC, Config) ->
    Mode = case TC of
        numpy_owngil_basic -> owngil;
        _                  -> worker
    end,
    case Mode of
        owngil ->
            case py_nif:owngil_supported() of
                false ->
                    {skip, "owngil mode requires Python 3.14+"};
                true ->
                    setup_case(TC, Mode, Config)
            end;
        worker ->
            setup_case(TC, Mode, Config)
    end.

end_per_testcase(_TC, Config) ->
    case proplists:get_value(ctx, Config) of
        undefined -> ok;
        Ctx -> py_context:stop(Ctx)
    end.

%%% ---------------------------------------------------------------------------
%%% Helpers
%%% ---------------------------------------------------------------------------

setup_case(TC, Mode, Config) ->
    case py_context:new(#{mode => Mode}) of
        {ok, Ctx} ->
            case require_module(Ctx, required_module(TC)) of
                ok ->
                    [{ctx, Ctx} | Config];
                {skip, _} = Skip ->
                    py_context:stop(Ctx),
                    Skip
            end;
        {error, Reason} ->
            ct:fail({context_create_failed, Mode, Reason})
    end.

required_module(numpy_basic_ops)              -> "numpy";
required_module(numpy_call_thread_affinity)   -> "numpy";
required_module(numpy_parallel_processes)     -> "numpy";
required_module(numpy_owngil_basic)           -> "numpy";
required_module(tensorflow_basic_ops)         -> "tensorflow";
required_module(tensorflow_call_thread_affinity) -> "tensorflow".

%% Reflect the import status into a Python variable so we can
%% distinguish "module not installed" (skip) from any other error
%% (let it bubble up). A native-extension crash that surfaces as a
%% non-ImportError must not silently turn into a skip.
require_module(Ctx, Mod) ->
    Code = iolist_to_binary([
        "try:\n",
        "    import ", Mod, "\n",
        "    _import_status = 'ok'\n",
        "except ImportError:\n",
        "    _import_status = 'not_found'\n"
    ]),
    ok = py_context:exec(Ctx, Code),
    {ok, Status} = py_context:eval(Ctx, <<"_import_status">>, #{}),
    case Status of
        <<"ok">> ->
            ok;
        <<"not_found">> ->
            {skip, "Python module " ++ Mod ++ " not available"}
    end.

native_id(Ctx) ->
    {ok, Tid} = py_context:eval(Ctx,
        <<"__import__('threading').get_native_id()">>, #{}),
    Tid.

%%% ---------------------------------------------------------------------------
%%% numpy cases
%%% ---------------------------------------------------------------------------

numpy_basic_ops(Config) ->
    Ctx = ?config(ctx, Config),
    %% Define a numpy-backed function and exercise both call and eval
    %% paths so a thread-state regression in either direction crashes.
    ok = py_context:exec(Ctx, <<
        "import numpy as np\n"
        "def vec_dot_self(xs):\n"
        "    v = np.array(xs, dtype=np.float64)\n"
        "    return float(np.dot(v, v))\n"
    >>),
    {ok, 30.0} = py_context:call(Ctx, '__main__', vec_dot_self,
                                  [[1.0, 2.0, 3.0, 4.0]]),
    ok = py_context:exec(Ctx,
        <<"_w = vec_dot_self([10.0, 0.0, 0.0])">>),
    {ok, 100.0} = py_context:eval(Ctx, <<"_w">>, #{}),
    ok.

numpy_call_thread_affinity(Config) ->
    Ctx = ?config(ctx, Config),
    ok = py_context:exec(Ctx, <<
        "import numpy as np\n"
        "import threading\n"
        "def numpy_with_tid(xs):\n"
        "    v = np.array(xs, dtype=np.float64)\n"
        "    return (threading.get_native_id(), float(np.sum(v)))\n"
    >>),
    Results = [py_context:call(Ctx, '__main__', numpy_with_tid,
                                [[float(I), float(I + 1), float(I + 2)]])
               || I <- lists:seq(1, 50)],
    Tids = [Tid || {ok, {Tid, _Sum}} <- Results],
    Sums = [Sum || {ok, {_Tid, Sum}} <- Results],
    50 = length(Tids),
    [SingleTid] = lists:usort(Tids),
    true = is_integer(SingleTid),
    %% Spot-check a few sums.
    Expected = [3.0 * I + 3.0 || I <- lists:seq(1, 50)],
    Expected = Sums,
    ok.

numpy_parallel_processes(Config) ->
    Ctx = ?config(ctx, Config),
    ok = py_context:exec(Ctx, <<
        "import numpy as np\n"
        "import threading\n"
        "def numpy_dot_with_tid(xs, ys):\n"
        "    a = np.array(xs, dtype=np.float64)\n"
        "    b = np.array(ys, dtype=np.float64)\n"
        "    return (threading.get_native_id(), float(np.dot(a, b)))\n"
    >>),
    Parent = self(),
    N = 8,
    Pids = [spawn_link(fun() ->
                  Xs = [float(K * J) || J <- lists:seq(1, 4)],
                  Ys = [float(K + J) || J <- lists:seq(1, 4)],
                  R = py_context:call(Ctx, '__main__', numpy_dot_with_tid,
                                       [Xs, Ys]),
                  Parent ! {result, K, R}
              end) || K <- lists:seq(1, N)],
    Results = [receive {result, K, R} -> {K, R} after 5000 -> ct:fail(timeout) end
               || _ <- Pids],
    %% All calls converged on one thread.
    Tids = [Tid || {_K, {ok, {Tid, _Sum}}} <- Results],
    N = length(Tids),
    [SingleTid] = lists:usort(Tids),
    true = is_integer(SingleTid),
    %% Each result matches the expected dot product.
    lists:foreach(fun({K, {ok, {_Tid, Got}}}) ->
                    Xs = [float(K * J) || J <- lists:seq(1, 4)],
                    Ys = [float(K + J) || J <- lists:seq(1, 4)],
                    Expected = lists:sum([X * Y || {X, Y} <- lists:zip(Xs, Ys)]),
                    true = abs(Got - Expected) < 1.0e-9
                  end, Results),
    %% Drain time + mailbox sanity (no orphan results).
    timer:sleep(50),
    {messages, []} = erlang:process_info(self(), messages),
    ok.

numpy_owngil_basic(Config) ->
    Ctx = ?config(ctx, Config),
    %% Same shape as numpy_basic_ops but inside an OWN_GIL subinterpreter.
    %% Numpy in OWN_GIL was the original v3.0 motivator on Python 3.14.
    ok = py_context:exec(Ctx, <<
        "import numpy as np\n"
        "def vec_dot_self(xs):\n"
        "    v = np.array(xs, dtype=np.float64)\n"
        "    return float(np.dot(v, v))\n"
    >>),
    {ok, 30.0} = py_context:call(Ctx, '__main__', vec_dot_self,
                                  [[1.0, 2.0, 3.0, 4.0]]),
    {ok, 25.0} = py_context:call(Ctx, '__main__', vec_dot_self, [[5.0]]),
    %% Confirm the thread is stable across owngil calls.
    Tid1 = native_id(Ctx),
    Tid2 = native_id(Ctx),
    Tid1 = Tid2,
    ok.

%%% ---------------------------------------------------------------------------
%%% tensorflow cases
%%% ---------------------------------------------------------------------------

tensorflow_basic_ops(Config) ->
    Ctx = ?config(ctx, Config),
    ok = py_context:exec(Ctx, <<
        "import tensorflow as tf\n"
        "def matmul_22():\n"
        "    a = tf.constant([[1.0, 2.0], [3.0, 4.0]])\n"
        "    return tf.linalg.matmul(a, a).numpy().tolist()\n"
    >>),
    {ok, [[7.0, 10.0], [15.0, 22.0]]} =
        py_context:call(Ctx, '__main__', matmul_22, []),
    ok.

tensorflow_call_thread_affinity(Config) ->
    Ctx = ?config(ctx, Config),
    ok = py_context:exec(Ctx, <<
        "import tensorflow as tf\n"
        "import threading\n"
        "def tf_sum_with_tid(xs):\n"
        "    t = tf.constant(xs, dtype=tf.float64)\n"
        "    return (threading.get_native_id(),\n"
        "            float(tf.math.reduce_sum(t).numpy()))\n"
    >>),
    Results = [py_context:call(Ctx, '__main__', tf_sum_with_tid,
                                [[float(I), float(I + 1)]])
               || I <- lists:seq(1, 20)],
    Tids = [Tid || {ok, {Tid, _Sum}} <- Results],
    20 = length(Tids),
    [SingleTid] = lists:usort(Tids),
    true = is_integer(SingleTid),
    ok.
