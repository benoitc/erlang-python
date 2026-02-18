%%% @doc Common Test suite for UDP/Datagram support in the event loop.
%%%
%%% Tests the UDP socket NIFs and integration with the Erlang-native
%%% asyncio event loop.
-module(py_udp_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_udp_socket_create/1,
    test_udp_send_recv/1,
    test_udp_echo/1,
    test_udp_multiple_clients/1,
    test_udp_broadcast_option/1,
    test_udp_read_callback/1
]).

all() ->
    [
        test_udp_socket_create,
        test_udp_send_recv,
        test_udp_echo,
        test_udp_multiple_clients,
        test_udp_broadcast_option,
        test_udp_read_callback
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% UDP Socket Creation Tests
%% ============================================================================

test_udp_socket_create(_Config) ->
    %% Create a UDP socket on an ephemeral port
    {ok, {Fd, Port}} = py_nif:create_test_udp_socket(0),
    true = is_integer(Fd),
    true = Fd > 0,
    true = is_integer(Port),
    true = Port > 0,

    %% Cleanup
    py_nif:close_test_fd(Fd),
    ok.

%% ============================================================================
%% UDP Send/Receive Tests
%% ============================================================================

test_udp_send_recv(_Config) ->
    %% Create two UDP sockets
    {ok, {ServerFd, ServerPort}} = py_nif:create_test_udp_socket(0),
    {ok, {ClientFd, _ClientPort}} = py_nif:create_test_udp_socket(0),

    %% Send data from client to server
    TestData = <<"hello udp">>,
    ok = py_nif:sendto_test_udp(ClientFd, TestData, <<"127.0.0.1">>, ServerPort),

    %% Give time for data to arrive
    timer:sleep(50),

    %% Receive on server
    {ok, {ReceivedData, {SourceHost, _SourcePort}}} = py_nif:recvfrom_test_udp(ServerFd, 1024),

    %% Verify data
    TestData = ReceivedData,
    <<"127.0.0.1">> = SourceHost,

    %% Cleanup
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(ClientFd),
    ok.

test_udp_echo(_Config) ->
    %% Create server and client sockets
    {ok, {ServerFd, ServerPort}} = py_nif:create_test_udp_socket(0),
    {ok, {ClientFd, _ClientPort}} = py_nif:create_test_udp_socket(0),

    %% Client sends data to server
    TestData = <<"echo test data">>,
    ok = py_nif:sendto_test_udp(ClientFd, TestData, <<"127.0.0.1">>, ServerPort),

    %% Give time for data to arrive
    timer:sleep(50),

    %% Server receives and echoes back
    {ok, {ReceivedData, {SourceHost, SourcePort}}} = py_nif:recvfrom_test_udp(ServerFd, 1024),
    TestData = ReceivedData,

    %% Echo back to client
    ok = py_nif:sendto_test_udp(ServerFd, ReceivedData, SourceHost, SourcePort),

    %% Give time for echo to arrive
    timer:sleep(50),

    %% Client receives echo
    {ok, {EchoData, _}} = py_nif:recvfrom_test_udp(ClientFd, 1024),

    %% Verify echo matches original
    TestData = EchoData,

    %% Cleanup
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(ClientFd),
    ok.

test_udp_multiple_clients(_Config) ->
    %% Create server socket
    {ok, {ServerFd, ServerPort}} = py_nif:create_test_udp_socket(0),

    %% Create multiple client sockets
    {ok, {Client1Fd, _Client1Port}} = py_nif:create_test_udp_socket(0),
    {ok, {Client2Fd, _Client2Port}} = py_nif:create_test_udp_socket(0),
    {ok, {Client3Fd, _Client3Port}} = py_nif:create_test_udp_socket(0),

    %% Send data from each client
    ok = py_nif:sendto_test_udp(Client1Fd, <<"client1">>, <<"127.0.0.1">>, ServerPort),
    ok = py_nif:sendto_test_udp(Client2Fd, <<"client2">>, <<"127.0.0.1">>, ServerPort),
    ok = py_nif:sendto_test_udp(Client3Fd, <<"client3">>, <<"127.0.0.1">>, ServerPort),

    %% Give time for data to arrive
    timer:sleep(100),

    %% Receive all messages on server
    {ok, {Data1, {_, Port1}}} = py_nif:recvfrom_test_udp(ServerFd, 1024),
    {ok, {Data2, {_, Port2}}} = py_nif:recvfrom_test_udp(ServerFd, 1024),
    {ok, {Data3, {_, Port3}}} = py_nif:recvfrom_test_udp(ServerFd, 1024),

    %% Verify we received all three messages (order may vary)
    ReceivedData = lists:sort([Data1, Data2, Data3]),
    ExpectedData = lists:sort([<<"client1">>, <<"client2">>, <<"client3">>]),
    ExpectedData = ReceivedData,

    %% Verify source ports are different (each client has unique port)
    Ports = lists:sort([Port1, Port2, Port3]),
    3 = length(lists:usort(Ports)),

    %% Cleanup
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(Client1Fd),
    py_nif:close_test_fd(Client2Fd),
    py_nif:close_test_fd(Client3Fd),
    ok.

%% ============================================================================
%% UDP Socket Options Tests
%% ============================================================================

test_udp_broadcast_option(_Config) ->
    %% Create a UDP socket
    {ok, {Fd, _Port}} = py_nif:create_test_udp_socket(0),

    %% Enable broadcast
    ok = py_nif:set_udp_broadcast(Fd, true),

    %% Disable broadcast
    ok = py_nif:set_udp_broadcast(Fd, false),

    %% Test with integer values
    ok = py_nif:set_udp_broadcast(Fd, 1),
    ok = py_nif:set_udp_broadcast(Fd, 0),

    %% Cleanup
    py_nif:close_test_fd(Fd),
    ok.

%% ============================================================================
%% UDP Event Loop Integration Tests
%% ============================================================================

test_udp_read_callback(_Config) ->
    {ok, LoopRef} = py_nif:event_loop_new(),
    {ok, RouterPid} = py_event_router:start_link(LoopRef),
    ok = py_nif:event_loop_set_router(LoopRef, RouterPid),

    %% Create UDP sockets
    {ok, {ServerFd, ServerPort}} = py_nif:create_test_udp_socket(0),
    {ok, {ClientFd, _ClientPort}} = py_nif:create_test_udp_socket(0),

    %% Register server fd for read monitoring
    ReadCallbackId = 500,
    {ok, FdRef} = py_nif:add_reader(LoopRef, ServerFd, ReadCallbackId),

    %% Send data from client
    ok = py_nif:sendto_test_udp(ClientFd, <<"callback test">>, <<"127.0.0.1">>, ServerPort),

    %% Wait for callback dispatch
    timer:sleep(100),

    %% Get pending events
    Pending = py_nif:get_pending(LoopRef),

    %% Should have received the read callback
    HasReadCallback = lists:any(
        fun({CId, Type}) -> CId == ReadCallbackId andalso Type == read end,
        Pending
    ),
    true = HasReadCallback,

    %% Cleanup - remove reader before closing fds
    ok = py_nif:remove_reader(LoopRef, FdRef),
    py_nif:close_test_fd(ServerFd),
    py_nif:close_test_fd(ClientFd),
    py_event_router:stop(RouterPid),
    py_nif:event_loop_destroy(LoopRef),
    ok.
