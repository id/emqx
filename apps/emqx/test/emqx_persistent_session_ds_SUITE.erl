%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_SUITE).

-feature(maybe_expr, enable).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/src/emqx_persistent_session_ds/session_internals.hrl").

-include("emqx_persistent_message.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(DURABLE_SESSION_STATE, emqx_persistent_session).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-ifdef(BUILD_WITH_FDB).
-define(SETUP_MOD, emqx_persistent_session_ds_SUITE_fdb_surrogate).
-else.
-define(SETUP_MOD, ?MODULE).
-endif.

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, cluster},
        {group, local}
    ].

init_per_suite(Config) ->
    case emqx_common_test_helpers:ensure_loaded(emqx_conf) of
        true ->
            Config;
        false ->
            {skip, standalone_not_supported}
    end.

end_per_suite(_Config) ->
    ok.

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    ClusterTCs = cluster_testcases(),
    [
        {cluster, ClusterTCs},
        {local, AllTCs -- ClusterTCs}
    ].

%% These are test cases that spin up their own clusters, even if they have a single node.
cluster_testcases() ->
    [
        t_session_subscription_idempotency,
        t_session_unsubscription_idempotency,
        t_subscription_state_change,
        t_storage_generations,
        t_new_stream_notifications,
        t_session_gc,
        t_crashed_node_session_gc,
        t_last_alive_at_cleanup
    ].

init_per_group(cluster, Config) ->
    %% Each test case sets up and tears down its cluster.
    Config;
init_per_group(local, Config) ->
    DurableSessionsOpts = #{
        <<"enable">> => true,
        <<"renew_streams_interval">> => <<"1s">>
    },
    Opts = #{
        durable_sessions_opts => DurableSessionsOpts,
        start_emqx_conf => false
    },
    emqx_common_test_helpers:start_apps_ds(Config, _ExtraApps = [], Opts).

end_per_group(cluster, _Config) ->
    ok;
end_per_group(local, Config) ->
    emqx_common_test_helpers:stop_apps_ds(Config),
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_session_subscription_idempotency;
    TestCase =:= t_session_unsubscription_idempotency;
    TestCase =:= t_subscription_state_change;
    TestCase =:= t_storage_generations;
    TestCase =:= t_new_stream_notifications
->
    %% Todo: un-skip `t_storage_generations' after we discover the issue.
    %% Context: At the time of writing, at `90cb15ed' this test was already inadvertently
    %% adding a new generation to the master CT node, but clients were connected to a
    %% remote peer.  After not starting the apps on the master CT node, adding the
    %% generation to the remote peer made this TC stop passing.
    case TestCase =:= t_storage_generations of
        true ->
            {skip, temp_skip_broken_test};
        false ->
            %% N.B.: these tests start a single-node cluster, so it's fine to test them with the
            %% `builtin_local' backend.
            DurableSessionsOpts = #{
                <<"heartbeat_interval">> => <<"500ms">>,
                <<"session_gc_interval">> => <<"1s">>,
                <<"session_gc_batch_size">> => 2
            },
            Opts = #{
                durable_sessions_opts => DurableSessionsOpts,
                work_dir => emqx_cth_suite:work_dir(TestCase, Config)
            },
            ClusterSpec = cluster(Opts#{n => 1}),
            emqx_common_test_helpers:start_cluster_ds(Config, ClusterSpec, Opts)
    end;
init_per_testcase(TestCase, Config) when
    TestCase =:= t_session_gc;
    TestCase =:= t_crashed_node_session_gc;
    TestCase =:= t_last_alive_at_cleanup
->
    try emqx_ds_test_helpers:skip_if_norepl() of
        false ->
            DurableSessionsOpts = #{
                <<"heartbeat_interval">> => <<"500ms">>,
                <<"session_gc_interval">> => <<"1s">>,
                <<"session_gc_batch_size">> => 2
            },
            Opts = #{
                durable_sessions_opts => DurableSessionsOpts,
                work_dir => emqx_cth_suite:work_dir(TestCase, Config)
            },
            NodeSpecs = cluster(Opts#{n => 3}),
            emqx_common_test_helpers:start_cluster_ds(Config, NodeSpecs, Opts);
        Skip ->
            Skip
    catch
        error:undef ->
            {skip, standalone_not_supported}
    end;
init_per_testcase(t_session_replay_retry, Config) ->
    maybe
        %% Todo: ideally, should find a way to "phrase" this test for platform build.
        false ?= emqx_common_test_helpers:skip_if_platform(),
        %% Todo: ideally, should find a way to "phrase" this test for emqx build.
        false ?= emqx_ds_test_helpers:skip_if_norepl(),
        Config
    end;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_session_subscription_idempotency;
    TestCase =:= t_session_unsubscription_idempotency;
    TestCase =:= t_subscription_state_change;
    TestCase =:= t_session_gc;
    TestCase =:= t_storage_generations;
    TestCase =:= t_new_stream_notifications;
    TestCase =:= t_crashed_node_session_gc;
    TestCase =:= t_last_alive_at_cleanup
->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(60_000),
    emqx_common_test_helpers:stop_cluster_ds(Config),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(60_000),
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Setup module callbacks
%%------------------------------------------------------------------------------

%% These callbacks are implemented in other repos by different modules.  Please do not
%% "refactor" and remove these nor change their API.
init_mock_transient_failure() ->
    ok = emqx_ds_test_helpers:mock_rpc().

mock_transient_failure() ->
    ok = emqx_ds_test_helpers:mock_rpc_result(
        fun
            (_Node, emqx_ds_replication_layer, _Function, [?DURABLE_SESSION_STATE, _Shard | _]) ->
                passthrough;
            (_Node, emqx_ds_replication_layer, _Function, [_DB, Shard | _]) ->
                case erlang:phash2(Shard) rem 2 of
                    0 -> unavailable;
                    1 -> passthrough
                end
        end
    ).

unmock_transient_failure() ->
    emqx_ds_test_helpers:unmock_rpc().

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

cluster(#{n := N} = Opts) ->
    MkRole = fun(M) ->
        case maps:get(roles, Opts, undefined) of
            undefined ->
                core;
            Roles ->
                lists:nth(M, Roles)
        end
    end,
    %% emqx is already started by test helper function
    MkSpec = fun(M) -> #{role => MkRole(M), apps => _ExtraApps = []} end,
    lists:map(
        fun(M) ->
            Name = list_to_atom("ds_SUITE" ++ integer_to_list(M)),
            {Name, MkSpec(M)}
        end,
        lists:seq(1, N)
    ).

app_specs() ->
    app_specs(_Opts = #{}).

app_specs(Opts) ->
    DefaultEMQXConf =
        "durable_sessions {enable = true}\n"
        "durable_storage.messages.local_write_buffer {max_items = 1, flush_interval = 1ms}\n",
    ExtraEMQXConf = maps:get(extra_emqx_conf, Opts, ""),
    [
        {emqx, DefaultEMQXConf ++ ExtraEMQXConf}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

wait_nodeup(Node) ->
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        pong = net_adm:ping(Node)
    ).

start_client(Opts0 = #{}) ->
    Defaults = #{
        port => 1883,
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    Opts = emqx_utils_maps:deep_merge(Defaults, Opts0),
    ?tp(notice, "starting client", Opts),
    {ok, Client} = emqtt:start_link(maps:to_list(Opts)),
    unlink(Client),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

start_connect_client(Opts = #{}) ->
    Client = start_client(Opts),
    ?assertMatch({ok, _}, emqtt:connect(Client)),
    Client.

mk_clientid(Prefix, ID) ->
    iolist_to_binary(io_lib:format("~p/~p", [Prefix, ID])).

restart_node(Node, NodeSpec) ->
    ?tp(will_restart_node, #{}),
    emqx_common_test_helpers:restart_node_ds(Node, NodeSpec),
    ?tp(restarted_node, #{}),
    ok.

is_persistent_connect_opts(#{properties := #{'Session-Expiry-Interval' := EI}}) ->
    EI > 0.

list_all_sessions(Node) ->
    erpc:call(Node, emqx_persistent_session_ds_state, list_sessions, []).

list_all_subscriptions(Node) ->
    Sessions = list_all_sessions(Node),
    lists:flatmap(
        fun(ClientId) ->
            #{s := #{subscriptions := Subs}} = erpc:call(
                Node, emqx_persistent_session_ds, print_session, [ClientId]
            ),
            maps:to_list(Subs)
        end,
        Sessions
    ).

list_all_pubranges(Node) ->
    erpc:call(Node, emqx_persistent_session_ds, list_all_pubranges, []).

session_open(Node, ClientId) ->
    ClientInfo = #{clientid => ClientId},
    ConnInfo = #{peername => {undefined, undefined}, proto_name => <<"MQTT">>, proto_ver => 5},
    WillMsg = undefined,
    Conf = #{
        max_subscriptions => infinity,
        max_awaiting_rel => infinity,
        upgrade_qos => false,
        retry_interval => 10,
        await_rel_timeout => infinity
    },
    {_, Sess, _} = erpc:call(
        Node,
        emqx_persistent_session_ds,
        open,
        [ClientInfo, ConnInfo, WillMsg, Conf]
    ),
    Sess.

force_last_alive_at(ClientId, Time) ->
    {ok, S0} = emqx_persistent_session_ds_state:open(ClientId),
    S = emqx_persistent_session_ds_state:set_last_alive_at(Time, S0),
    _ = emqx_persistent_session_ds_state:commit(S),
    ok.

stop_and_commit(Client) ->
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:stop(Client),
            #{?snk_kind := ?sessds_terminate}
        ),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% This testcase verifies session's behavior related to generation
%% rotation in the message storage.
%%
%% This testcase verifies (on the surface level) that session handles
%% `end_of_stream' and doesn't violate the ordering of messages that
%% are split into different generations.
t_storage_generations(Config) ->
    [Node1] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    TopicFilter = <<"t/+">>,
    ClientId = mk_clientid(?FUNCTION_NAME, sub),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Start subscriber:
            Sub = start_client(#{port => Port, clientid => ClientId, auto_ack => never}),
            {ok, _} = emqtt:connect(Sub),
            {ok, _, _} = emqtt:subscribe(Sub, TopicFilter, qos2),
            %% Start publisher:
            Pub = start_client(#{port => Port, clientid => mk_clientid(?FUNCTION_NAME, pub)}),
            {ok, _} = emqtt:connect(Pub),
            %% Publish 3 messages. Subscriber receives them, but
            %% doesn't ack them initially.
            {ok, _} = emqtt:publish(Pub, <<"t/1">>, <<"1">>, ?QOS_1),
            [
                #{packet_id := PI1, payload := <<"1">>}
            ] = emqx_common_test_helpers:wait_publishes(1, 5_000),
            {ok, _} = emqtt:publish(Pub, <<"t/2">>, <<"2">>, ?QOS_1),
            {ok, _} = emqtt:publish(Pub, <<"t/2">>, <<"3">>, ?QOS_1),
            [
                #{packet_id := PI2, payload := <<"2">>},
                #{packet_id := PI3, payload := <<"3">>}
            ] = emqx_common_test_helpers:wait_publishes(2, 5_000),
            %% Ack the first message. It transfers "t/1" stream into
            %% ready state where it will be polled.
            ok = emqtt:puback(Sub, PI1),
            %% Create a new generation and publish messages to the new
            %% generation. We expect 1 message for topic t/1 (since it
            %% was unblocked), but NOT for t/2:
            ok = ?ON(Node1, emqx_ds:add_generation(?PERSISTENT_MESSAGE_DB)),
            timer:sleep(100),
            {ok, _} = emqtt:publish(Pub, <<"t/1">>, <<"4">>, ?QOS_1),
            {ok, _} = emqtt:publish(Pub, <<"t/2">>, <<"5">>, ?QOS_1),
            [
                #{packet_id := PI4, payload := <<"4">>}
            ] = emqx_common_test_helpers:wait_publishes(2, 5_000),
            %% Ack the rest of messages, it should unblock 5th
            %% message:
            ok = emqtt:puback(Sub, PI2),
            ok = emqtt:puback(Sub, PI3),
            ok = emqtt:puback(Sub, PI4),
            [#{packet_id := _PI5}] = emqx_common_test_helpers:wait_publishes(1, 5_000)
        end,
        []
    ),
    ok.

t_session_subscription_idempotency(Config) ->
    [Node1Spec | _] = ?config(node_specs, Config),
    [Node1] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    SubTopicFilter = <<"t/+">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?force_ordering(
                #{?snk_kind := persistent_session_ds_subscription_added},
                _NEvents0 = 1,
                #{?snk_kind := will_restart_node},
                _Guard0 = true
            ),
            ?force_ordering(
                #{?snk_kind := restarted_node},
                _NEvents1 = 1,
                #{?snk_kind := persistent_session_ds_open_iterators, ?snk_span := start},
                _Guard1 = true
            ),

            spawn_link(fun() -> restart_node(Node1, Node1Spec) end),

            ?tp(notice, "starting 1", #{}),
            Client0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing 1", #{}),
            process_flag(trap_exit, true),
            catch emqtt:subscribe(Client0, SubTopicFilter, qos2),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 100 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ?tp(notice, "starting 2", #{}),
            Client1 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client1),
            ?tp(notice, "subscribing 2", #{}),
            {ok, _, [2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),

            ok = emqtt:stop(Client1),

            ok
        end,
        fun(_Trace) ->
            Session = session_open(Node1, ClientId),
            ?assertMatch(
                #{SubTopicFilter := #{}},
                emqx_session:info(subscriptions, Session)
            )
        end
    ),
    ok.

%% Check that we close the iterators before deleting the iterator id entry.
t_session_unsubscription_idempotency(Config) ->
    [Node1Spec | _] = ?config(node_specs, Config),
    [Node1] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    SubTopicFilter = <<"t/+">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?force_ordering(
                #{
                    ?snk_kind := persistent_session_ds_subscription_delete
                },
                _NEvents0 = 1,
                #{?snk_kind := will_restart_node},
                _Guard0 = true
            ),
            ?force_ordering(
                #{?snk_kind := restarted_node},
                _NEvents1 = 1,
                #{?snk_kind := persistent_session_ds_subscription_route_delete, ?snk_span := start},
                _Guard1 = true
            ),

            spawn_link(fun() -> restart_node(Node1, Node1Spec) end),

            ?tp(notice, "starting 1", #{}),
            Client0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing 1", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client0, SubTopicFilter, qos2),
            ?tp(notice, "unsubscribing 1", #{}),
            process_flag(trap_exit, true),
            catch emqtt:unsubscribe(Client0, SubTopicFilter),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 100 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ?tp(notice, "starting 2", #{}),
            Client1 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client1),
            ?tp(notice, "subscribing 2", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),
            ?tp(notice, "unsubscribing 2", #{}),
            {{ok, _, [?RC_SUCCESS]}, {ok, _}} =
                ?wait_async_action(
                    emqtt:unsubscribe(Client1, SubTopicFilter),
                    #{
                        ?snk_kind := persistent_session_ds_subscription_route_delete,
                        ?snk_span := {complete, _}
                    },
                    15_000
                ),

            ok = stop_and_commit(Client1)
        end,
        fun(_Trace) ->
            Session = session_open(Node1, ClientId),
            ?assertEqual(
                #{},
                emqx_session:info(subscriptions, Session)
            ),
            ok
        end
    ),
    ok.

%% This testcase verifies that the session handles update of the
%% subscription settings by the client correctly.
t_subscription_state_change(Config) ->
    [Node1] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    TopicFilter = <<"t/+">>,
    ClientId = mk_clientid(?FUNCTION_NAME, sub),
    %% Helper function that forces session to perform garbage
    %% collection. In the current implementation it happens before
    %% session state is synced to the DB:
    WaitGC = fun() ->
        erpc:call(Node1, emqx_persistent_session_ds, sync, [ClientId])
    end,
    %% Helper function that gets runtime state of the session:
    GetS = fun() ->
        maps:get(s, erpc:call(Node1, emqx_persistent_session_ds, print_session, [ClientId]))
    end,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Init:
            Sub = start_client(#{port => Port, clientid => ClientId, auto_ack => never}),
            {ok, _} = emqtt:connect(Sub),
            Pub = start_client(#{port => Port, clientid => mk_clientid(?FUNCTION_NAME, pub)}),
            {ok, _} = emqtt:connect(Pub),
            %% Subscribe to the topic using QoS1 initially:
            {ok, _, _} = emqtt:subscribe(Sub, TopicFilter, ?QOS_1),
            #{subscriptions := Subs1} = GetS(),
            #{TopicFilter := #{current_state := SSID1}} = Subs1,
            %% Fill the storage with some data to create the streams:
            {ok, _} = emqtt:publish(Pub, <<"t/1">>, <<"1">>, ?QOS_2),
            [#{packet_id := PI1, qos := ?QOS_1, topic := <<"t/1">>, payload := <<"1">>}] =
                emqx_common_test_helpers:wait_publishes(1, 5_000),
            %% Upgrade subscription to QoS2 and wait for the session GC
            %% to happen. At which point the session should keep 2
            %% subscription states, one being marked as obsolete:
            {ok, _, _} = emqtt:subscribe(Sub, TopicFilter, ?QOS_2),
            ok = WaitGC(),
            #{subscriptions := Subs2, subscription_states := SStates2} = GetS(),
            #{TopicFilter := #{current_state := SSID2}} = Subs2,
            ?assertNotEqual(SSID1, SSID2),
            ?assertMatch(
                #{
                    SSID1 := #{subopts := #{qos := ?QOS_1}, superseded_by := SSID2},
                    SSID2 := #{subopts := #{qos := ?QOS_2}}
                },
                SStates2
            ),
            %% Now ack the packet and trigger garbage collection. This
            %% should release the reference to the old subscription
            %% state, and let GC delete old subscription state:
            ok = emqtt:puback(Sub, PI1),
            {ok, _} = emqtt:publish(Pub, <<"t/1">>, <<"2">>, ?QOS_2),
            %% Verify that QoS of subscription has been updated:
            [#{packet_id := _PI2, qos := ?QOS_2, topic := <<"t/1">>, payload := <<"2">>}] =
                emqx_common_test_helpers:wait_publishes(1, 5_000),
            ok = WaitGC(),
            #{subscriptions := Subs3, subscription_states := SStates3, streams := Streams3} = GetS(),
            ?assertEqual(Subs3, Subs2),
            %% Verify that the old substate (SSID1) was deleted:
            ?assertMatch(
                [{SSID2, _}],
                maps:to_list(SStates3),
                #{
                    subs => Subs3,
                    streams => Streams3,
                    ssid1 => SSID1,
                    ssid2 => SSID2,
                    sub_states => SStates3
                }
            )
        end,
        []
    ).

%% This testcase verifies the lifetimes of session's subscriptions to
%% new stream events.
t_new_stream_notifications(Config) ->
    [Node1] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    ClientId = mk_clientid(?FUNCTION_NAME, sub),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Init:
            Sub0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Sub0),
            %% 1. Sessions should start watching streams when they
            %% subscribe to the topics:
            ?wait_async_action(
                {ok, _, _} = emqtt:subscribe(Sub0, <<"foo/+">>, ?QOS_1),
                #{?snk_kind := ?sessds_sched_watch_streams, topic_filter := [<<"foo">>, '+']}
            ),
            ?wait_async_action(
                {ok, _, _} = emqtt:subscribe(Sub0, <<"bar">>, ?QOS_1),
                #{?snk_kind := ?sessds_sched_watch_streams, topic_filter := [<<"bar">>]}
            ),
            %% 2. Sessions should re-subscribe to the events after
            %% reconnect:
            emqtt:disconnect(Sub0),
            {ok, SNKsub} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := ?sessds_sched_watch_streams}),
                2,
                timer:seconds(10)
            ),
            Sub1 = start_client(#{port => Port, clientid => ClientId, clean_start => false}),
            {ok, _} = emqtt:connect(Sub1),
            %% Verify that both subscriptions have been renewed:
            {ok, EventsAfterRestart} = snabbkaffe:receive_events(SNKsub),
            ?assertMatch(
                [[<<"bar">>], [<<"foo">>, '+']],
                lists:sort(?projection(topic_filter, EventsAfterRestart))
            ),
            %% Verify that stream notifications are handled:
            ?wait_async_action(
                emqx_ds_new_streams:notify_new_stream(?PERSISTENT_MESSAGE_DB, [<<"bar">>]),
                #{?snk_kind := ?sessds_sched_renew_streams, topic_filter := [<<"bar">>]}
            ),
            ?wait_async_action(
                emqx_ds_new_streams:notify_new_stream(?PERSISTENT_MESSAGE_DB, [<<"foo">>, <<"1">>]),
                #{?snk_kind := ?sessds_sched_renew_streams, topic_filter := [<<"foo">>, '+']}
            ),
            %% Verify that new stream subscriptions are removed when
            %% session unsubscribes from a topic:
            ?wait_async_action(
                emqtt:unsubscribe(Sub1, <<"bar">>),
                #{?snk_kind := ?sessds_sched_unwatch_streams, topic_filter := [<<"bar">>]}
            ),
            %% But the rest of subscriptions are still active:
            ?wait_async_action(
                emqx_ds_new_streams:set_dirty(?PERSISTENT_MESSAGE_DB),
                #{?snk_kind := ?sessds_sched_renew_streams, topic_filter := [<<"foo">>, '+']}
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [], ?of_kind(sessds_new_stream_notification_for_undefined_subscription, Trace)
            ),
            ?assertMatch(
                [], ?of_kind(sessds_unexpected_stream_notification, Trace)
            )
        end
    ).

t_fuzz(_Config) ->
    snabbkaffe:fix_ct_logging(),
    %% NOTE: we set timeout at the lower level to capture the trace
    %% and have a nicer error message.
    ?run_prop(
        #{
            proper => #{
                timeout => 3_000_000,
                numtests => 100,
                max_size => 1000,
                start_size => 100,
                max_shrinks => 0
            }
        },
        ?forall_trace(
            Cmds,
            proper_statem:commands(emqx_persistent_session_ds_fuzzer),
            #{timetrap => 60_000},
            try
                %% Initialize DS:
                ok = emqx_persistent_message:init(),
                %% FIXME: get_streams troubleshooting
                timer:sleep(1000),
                %% FIXME: this should not be needed, but some crap is
                %% left behind sometimes.
                emqx_persistent_session_ds_fuzzer:cleanup(),
                %% Run test:
                {_History, State, Result} = proper_statem:run_commands(
                    emqx_persistent_session_ds_fuzzer, Cmds
                ),
                SessState = emqx_persistent_session_ds_fuzzer:sut_state(),
                Result =:= ok orelse
                    begin
                        logger:error("*** Commands:~n~s~n", [
                            emqx_persistent_session_ds_fuzzer:print_cmds(Cmds)
                        ]),
                        logger:error("*** Model state:~n  ~p~n", [State]),
                        logger:error("*** Session state:~n  ~p~n", [SessState]),
                        logger:error("*** Result:~n  ~p~n", [Result]),
                        error(Result)
                    end
            after
                ok = emqx_persistent_session_ds_fuzzer:cleanup(),
                ok = emqx_ds:drop_db(?PERSISTENT_MESSAGE_DB)
            end,
            [
                fun emqx_persistent_session_ds_fuzzer:tprop_packet_id_history/1,
                fun emqx_persistent_session_ds_fuzzer:tprop_qos12_delivery/1,
                fun no_abnormal_session_terminate/1
                | emqx_persistent_session_ds:trace_specs()
            ]
        )
    ),
    snabbkaffe:stop().

t_session_discard_persistent_to_non_persistent(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Params = #{
        client_id => ClientId,
        reconnect_opts =>
            #{
                clean_start => true,
                %% we set it to zero so that a new session is not created.
                properties => #{'Session-Expiry-Interval' => 0},
                proto_ver => v5
            }
    },
    do_t_session_discard(Params).

t_session_discard_persistent_to_persistent(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Params = #{
        client_id => ClientId,
        reconnect_opts =>
            #{
                clean_start => true,
                properties => #{'Session-Expiry-Interval' => 30},
                proto_ver => v5
            }
    },
    do_t_session_discard(Params).

do_t_session_discard(Params) ->
    #{
        client_id := ClientId,
        reconnect_opts := ReconnectOpts0
    } = Params,
    ReconnectOpts = ReconnectOpts0#{clientid => ClientId},
    SubTopicFilter = <<"t/+">>,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?tp(notice, "starting", #{}),
            Client0 = start_client(#{
                clientid => ClientId,
                clean_start => true,
                properties => #{'Session-Expiry-Interval' => 30},
                proto_ver => v5
            }),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client0, SubTopicFilter, qos2),
            %% Store some matching messages so that streams and iterators are created.
            ok = emqtt:publish(Client0, <<"t/1">>, <<"1">>),
            ok = emqtt:publish(Client0, <<"t/2">>, <<"2">>),
            ?retry(
                _Sleep0 = 100,
                _Attempts0 = 50,
                #{} = emqx_persistent_session_ds_state:print_session(ClientId)
            ),
            ok = emqtt:stop(Client0),
            ?tp(notice, "disconnected", #{}),

            ?tp(notice, "reconnecting", #{}),
            %% we still have the session:
            ?assertMatch(#{}, emqx_persistent_session_ds_state:print_session(ClientId)),
            Client1 = start_client(ReconnectOpts),
            {ok, _} = emqtt:connect(Client1),
            ?assertEqual([], emqtt:subscriptions(Client1)),
            case is_persistent_connect_opts(ReconnectOpts) of
                true ->
                    ?assertMatch(#{}, emqx_persistent_session_ds_state:print_session(ClientId));
                false ->
                    ?assertEqual(
                        undefined, emqx_persistent_session_ds_state:print_session(ClientId)
                    )
            end,
            ?assertEqual([], emqx_persistent_session_ds_router:topics()),
            ok = emqtt:stop(Client1),
            ?tp(notice, "disconnected", #{}),

            ok
        end,
        []
    ),
    ok.

t_session_expiration1(Config) ->
    %% This testcase verifies that the properties passed in the
    %% CONNECT packet are respected by the GC process:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Opts = #{
        clientid => ClientId,
        sequence => [
            {#{clean_start => false, properties => #{'Session-Expiry-Interval' => 30}}, #{}},
            {#{clean_start => false, properties => #{'Session-Expiry-Interval' => 1}}, #{}},
            {#{clean_start => false, properties => #{'Session-Expiry-Interval' => 30}}, #{}}
        ]
    },
    do_t_session_expiration(Config, Opts).

t_session_expiration2(Config) ->
    %% This testcase updates the expiry interval for the session in
    %% the _DISCONNECT_ packet. This setting should be respected by GC
    %% process:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Opts = #{
        clientid => ClientId,
        sequence => [
            {#{clean_start => false, properties => #{'Session-Expiry-Interval' => 30}}, #{}},
            {#{clean_start => false, properties => #{'Session-Expiry-Interval' => 30}}, #{
                'Session-Expiry-Interval' => 1
            }},
            {#{clean_start => false, properties => #{'Session-Expiry-Interval' => 30}}, #{}}
        ]
    },
    do_t_session_expiration(Config, Opts).

do_t_session_expiration(_Config, Opts) ->
    %% Sequence is a list of pairs of properties passed through the
    %% CONNECT and for the DISCONNECT for each session:
    #{
        clientid := ClientId,
        sequence := [
            {FirstConn, FirstDisconn},
            {SecondConn, SecondDisconn},
            {ThirdConn, ThirdDisconn}
        ]
    } = Opts,
    CommonParams = #{proto_ver => v5, clientid => ClientId},
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Topic = <<"some/topic">>,
            Params0 = maps:merge(CommonParams, FirstConn),
            Client0 = start_client(Params0),
            {ok, _} = emqtt:connect(Client0),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client0, Topic, ?QOS_2),
            #{s := #{subscriptions := Subs0}} = emqx_persistent_session_ds:print_session(ClientId),
            ?assertEqual(1, map_size(Subs0), #{subs => Subs0}),
            Info0 = maps:from_list(emqtt:info(Client0)),
            ?assertEqual(0, maps:get(session_present, Info0), #{info => Info0}),
            emqtt:disconnect(Client0, ?RC_NORMAL_DISCONNECTION, FirstDisconn),

            Params1 = maps:merge(CommonParams, SecondConn),
            Client1 = start_client(Params1),
            {ok, _} = emqtt:connect(Client1),
            Info1 = maps:from_list(emqtt:info(Client1)),
            ?assertEqual(1, maps:get(session_present, Info1), #{info => Info1}),
            Subs1 = emqtt:subscriptions(Client1),
            ?assertEqual([], Subs1),
            emqtt:disconnect(Client1, ?RC_NORMAL_DISCONNECTION, SecondDisconn),

            ct:sleep(2_500),

            Params2 = maps:merge(CommonParams, ThirdConn),
            Client2 = start_client(Params2),
            {ok, _} = emqtt:connect(Client2),
            Info2 = maps:from_list(emqtt:info(Client2)),
            ?assertEqual(0, maps:get(session_present, Info2), #{info => Info2}),
            Subs2 = emqtt:subscriptions(Client2),
            ?assertEqual([], Subs2),
            emqtt:publish(Client2, Topic, <<"payload">>),
            ?assertNotReceive({publish, #{topic := Topic}}),
            %% ensure subscriptions are absent from table.
            #{s := #{subscriptions := Subs3}} = emqx_persistent_session_ds:print_session(ClientId),
            ?assertEqual([], maps:to_list(Subs3)),
            emqtt:disconnect(Client2, ?RC_NORMAL_DISCONNECTION, ThirdDisconn),
            ok
        end,
        []
    ),
    ok.

t_session_gc(Config) ->
    [Node1, _Node2, _Node3] = Nodes = ?config(cluster_nodes, Config),
    [
        Port1,
        Port2,
        Port3
    ] = lists:map(fun(N) -> get_mqtt_port(N, tcp) end, Nodes),
    ct:pal("Ports: ~p", [[Port1, Port2, Port3]]),
    CommonParams = #{
        clean_start => false,
        proto_ver => v5
    },
    StartClient = fun(ClientId, Port, ExpiryInterval) ->
        Params = maps:merge(CommonParams, #{
            clientid => ClientId,
            port => Port,
            properties => #{'Session-Expiry-Interval' => ExpiryInterval}
        }),
        Client = start_client(Params),
        {ok, _} = emqtt:connect(Client),
        Client
    end,

    ?check_trace(
        #{timetrap => 30_000},
        begin
            ClientId1 = <<"session_gc1">>,
            Client1 = StartClient(ClientId1, Port1, 30),

            ClientId2 = <<"session_gc2">>,
            Client2 = StartClient(ClientId2, Port2, 1),

            ClientId3 = <<"session_gc3">>,
            Client3 = StartClient(ClientId3, Port3, 1),

            lists:foreach(
                fun(Client) ->
                    Topic = <<"some/topic">>,
                    Payload = <<"hi">>,
                    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client, Topic, ?QOS_1),
                    {ok, _} = emqtt:publish(Client, Topic, Payload, ?QOS_1),
                    ok
                end,
                [Client1, Client2, Client3]
            ),

            %% Clients are still alive; no session is garbage collected.
            ?tp(notice, "waiting for gc", #{}),
            ?assertMatch(
                {ok, _},
                ?block_until(
                    #{
                        ?snk_kind := ds_session_gc,
                        ?snk_span := {complete, _},
                        ?snk_meta := #{node := N}
                    } when N =/= node()
                )
            ),
            ?assertMatch([_, _, _], list_all_sessions(Node1), sessions),
            ?assertMatch([_, _, _], list_all_subscriptions(Node1), subscriptions),
            ?tp(notice, "gc ran", #{}),

            %% Now we disconnect 2 of them; only those should be GC'ed.

            ?tp(notice, "disconnecting client1", #{}),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqtt:stop(Client2),
                    #{?snk_kind := ?sessds_terminate}
                )
            ),
            ?tp(notice, "disconnected client1", #{}),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqtt:stop(Client3),
                    #{?snk_kind := ?sessds_terminate}
                )
            ),
            ?tp(notice, "disconnected client2", #{}),
            ?assertMatch(
                {ok, _},
                ?block_until(
                    #{
                        ?snk_kind := ds_session_gc_cleaned,
                        session_id := ClientId2
                    }
                )
            ),
            ?assertMatch(
                {ok, _},
                ?block_until(
                    #{
                        ?snk_kind := ds_session_gc_cleaned,
                        session_id := ClientId3
                    }
                )
            ),
            ?retry(50, 3, [ClientId1] = list_all_sessions(Node1)),
            ?assertMatch([_], list_all_subscriptions(Node1), subscriptions),
            ok
        end,
        []
    ),
    ok.

t_crashed_node_session_gc(Config) ->
    [Node1, Node2 | _] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    ct:pal("Port: ~p", [Port]),

    ?check_trace(
        #{timetrap => 30_000},
        begin
            ClientId = <<"session_on_crashed_node">>,
            Client = start_client(#{
                clientid => ClientId,
                port => Port,
                properties => #{'Session-Expiry-Interval' => 1},
                clean_start => false,
                proto_ver => v5
            }),
            {ok, _} = emqtt:connect(Client),
            ct:sleep(1500),
            emqx_cth_peer:kill(Node1),

            %% Much time has passed since the session last reported its alive time (on start).
            %% Last alive time was not persisted on connection shutdown since we brutally killed the node.
            %% However, the session should not be expired,
            %% because session's last alive time should be bumped by the node's last_alive_at, and
            %% the node only recently crashed.
            erpc:call(Node2, emqx_persistent_session_ds_gc_worker, check_session, [ClientId]),
            %%% Wait for possible async dirty session delete
            ct:sleep(100),
            ?assertMatch([_], list_all_sessions(Node2), sessions),

            %% But finally the session has to expire since the connection
            %% is not re-established.
            ?assertMatch(
                {ok, _},
                ?block_until(
                    #{
                        ?snk_kind := ds_session_gc_cleaned,
                        session_id := ClientId
                    }
                )
            ),
            %%% Wait for possible async dirty session delete
            ct:sleep(100),
            ?assertMatch([], list_all_sessions(Node2), sessions)
        end,
        []
    ),
    ok.

t_last_alive_at_cleanup(Config) ->
    [Node1 | _] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    ?check_trace(
        #{timetrap => 5_000},
        begin
            NodeEpochId = erpc:call(
                Node1,
                emqx_persistent_session_ds_node_heartbeat_worker,
                get_node_epoch_id,
                []
            ),
            ClientId = <<"session_on_crashed_node">>,
            Client = start_client(#{
                clientid => ClientId,
                port => Port,
                properties => #{'Session-Expiry-Interval' => 1},
                clean_start => false,
                proto_ver => v5
            }),
            {ok, _} = emqtt:connect(Client),

            %% Kill node making its lifetime epoch invalid.
            emqx_cth_peer:kill(Node1),

            %% Wait till the node's epoch is cleaned up.
            ?assertMatch(
                {ok, _},
                ?block_until(
                    #{
                        ?snk_kind := persistent_session_ds_node_heartbeat_delete_epochs,
                        epoch_ids := [NodeEpochId]
                    }
                )
            )
        end,
        []
    ),
    ok.

tt_session_replay_retry(_Config) ->
    %% Verify that the session recovers smoothly from transient errors during
    %% replay.

    ?SETUP_MOD:init_mock_transient_failure(),

    NClients = 10,
    ClientSubOpts = #{
        clientid => mk_clientid(?FUNCTION_NAME, sub),
        auto_ack => never
    },
    ClientSub = start_connect_client(ClientSubOpts),
    ?assertMatch(
        {ok, _, [?RC_GRANTED_QOS_1]},
        emqtt:subscribe(ClientSub, <<"t/#">>, ?QOS_1)
    ),

    ClientsPub = [
        start_connect_client(#{
            clientid => mk_clientid(?FUNCTION_NAME, I),
            properties => #{'Session-Expiry-Interval' => 0}
        })
     || I <- lists:seq(1, NClients)
    ],
    lists:foreach(
        fun(Client) ->
            Index = integer_to_binary(rand:uniform(NClients)),
            Topic = <<"t/", Index/binary>>,
            ?assertMatch({ok, #{}}, emqtt:publish(Client, Topic, Index, 1))
        end,
        ClientsPub
    ),

    Pubs0 = emqx_common_test_helpers:wait_publishes(NClients, 5_000),
    NPubs = length(Pubs0),
    ?assertEqual(NClients, NPubs, ?drainMailbox(2_500)),

    ok = emqtt:stop(ClientSub),

    %% Make `emqx_ds` believe that roughly half of the shards are unavailable.
    ?SETUP_MOD:mock_transient_failure(),

    _ClientSub = start_connect_client(ClientSubOpts#{clean_start => false}),

    Pubs1 = emqx_common_test_helpers:wait_publishes(NPubs, 5_000),
    ?assert(length(Pubs1) < length(Pubs0), #{
        num_pubs1 => length(Pubs1),
        num_pubs0 => length(Pubs0),
        pubs1 => Pubs1,
        pubs0 => Pubs0
    }),

    %% "Recover" the shards.
    ?SETUP_MOD:unmock_transient_failure(),

    Pubs2 = emqx_common_test_helpers:wait_publishes(NPubs - length(Pubs1), 5_000),
    ?assertEqual(
        [maps:with([topic, payload, qos], P) || P <- Pubs0],
        [maps:with([topic, payload, qos], P) || P <- Pubs1 ++ Pubs2]
    ),
    ok.

%% Check that we send will messages when performing GC without relying on timers set by
%% the channel process.
t_session_gc_will_message(_Config) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            WillTopic = <<"will/t">>,
            ok = emqx:subscribe(WillTopic, #{qos => 2}),
            ClientId = <<"will_msg_client">>,
            Client = start_client(#{
                clientid => ClientId,
                will_topic => WillTopic,
                will_payload => <<"will payload">>,
                will_qos => 0,
                will_props => #{'Will-Delay-Interval' => 300}
            }),
            {ok, _} = emqtt:connect(Client),
            %% Use reason code =/= `?RC_SUCCESS' to allow will message
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqtt:disconnect(Client, ?RC_UNSPECIFIED_ERROR),
                    #{?snk_kind := emqx_cm_clean_down}
                ),
            ?assertNotReceive({deliver, WillTopic, _}),
            %% Set fake `last_alive_at' to trigger immediate will message.
            force_last_alive_at(ClientId, _Time = 0),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_persistent_session_ds_gc_worker:check_session(ClientId),
                    #{?snk_kind := session_gc_published_will_msg}
                ),
            ?assertReceive({deliver, WillTopic, _}),

            ok
        end,
        []
    ),
    ok.

%% Trace specifications:

%% @doc Verify that sessions didn't terminate abnormally. Note: it's
%% not part of emqx_persistent_session_ds:trace_props because,
%% strictly speaking, in the real world session may be terminated for
%% many reasons.
no_abnormal_session_terminate(Trace) ->
    lists:foreach(
        fun
            (#{?snk_kind := ?sessds_terminate} = E) ->
                case E of
                    #{reason := takenover} -> ok;
                    #{reason := kicked} -> ok;
                    #{reason := {shutdown, tcp_closed}} -> ok
                end;
            (_) ->
                ok
        end,
        Trace
    ).

%% check_stream_state_transitions(Trace) ->
%%     %% Check sequence of state transitions for each stream replay
%%     %% state:
%%     Groups = maps:groups_from_list(
%%         fun(#{key := Key, ?snk_meta := #{clientid := ClientId}}) -> {ClientId, Key} end,
%%         fun(#{to := To}) -> To end,
%%         ?of_kind(sessds_stream_state_trans, Trace)
%%     ),
%%     ?assert(maps:size(Groups) > 0),
%%     maps:foreach(
%%         fun(StreamId, Transitions) ->
%%             check_stream_state_transitions(StreamId, Transitions, void)
%%         end,
%%         Groups
%%     ).

%% %% erlfmt-ignore
%% check_stream_state_transitions(_StreamId, [], _) ->
%%     true;
%% check_stream_state_transitions(StreamId = {ClientId, Key}, ['$restore', To | Rest], State) ->
%%     %% This clause verifies that restored session re-calculates states
%%     %% of the streams exactly as they were before.
%%     case To of
%%         State ->
%%             check_stream_state_transitions(StreamId, Rest, State);
%%         _ ->
%%             error(#{
%%                 kind => inconsistent_stream_state_after_session_restore,
%%                 from => State,
%%                 to => To,
%%                 clientid => ClientId,
%%                 key => Key
%%             })
%%     end;
%% check_stream_state_transitions(StreamId = {ClientId, Key}, [To | Rest], State) ->
%%     %% See FSM in emqx_persistent_session_ds_stream_scheduler.erl:
%%     case {State, To} of
%%         {void, r} -> ok;
%%         %% R
%%         {r, p} -> ok;
%%         {r, u} -> ok;
%%         %% P
%%         {p, r} -> ok;
%%         {p, s} -> ok;
%%         %% S
%%         {s, r} -> ok;
%%         {s, u} -> ok;
%%         {s, bq1} -> ok;
%%         {s, bq2} -> ok;
%%         {s, bq12} -> ok;
%%         %% BQ1
%%         {bq1, u} -> ok;
%%         {bq1, r} -> ok;
%%         %% BQ2
%%         {bq2, u} -> ok;
%%         {bq2, r} -> ok;
%%         %% BQ12
%%         {bq12, u} -> ok;
%%         {bq12, bq1} -> ok;
%%         {bq12, bq2} -> ok;
%%         _ ->
%%             error(#{
%%                 kind => invalid_state_transition,
%%                 from => State,
%%                 to => To,
%%                 clientid => ClientId,
%%                 key => Key
%%             })
%%     end,
%%     check_stream_state_transitions(StreamId, Rest, To).
