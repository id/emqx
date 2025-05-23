%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_broker_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                override_env => [{boot_modules, [broker]}]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    emqx_broker_helper:start_link(),
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_lookup_subid(_) ->
    ?assertEqual(undefined, emqx_broker_helper:lookup_subid(self())),
    emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ?assertEqual(<<"clientid">>, emqx_broker_helper:lookup_subid(self())).

t_lookup_subpid(_) ->
    ?assertEqual(undefined, emqx_broker_helper:lookup_subpid(<<"clientid">>)),
    emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ?assertEqual(self(), emqx_broker_helper:lookup_subpid(<<"clientid">>)).

t_register_sub(_) ->
    ok = emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ok = emqx_broker_helper:register_sub(self(), <<"clientid">>),
    try emqx_broker_helper:register_sub(self(), <<"clientid2">>) of
        _ -> ct:fail(should_throw_error)
    catch
        error:Reason ->
            ?assertEqual(Reason, subid_conflict)
    end,
    ?assertEqual(self(), emqx_broker_helper:lookup_subpid(<<"clientid">>)).

t_shard_seq(_) ->
    TestTopic = atom_to_list(?FUNCTION_NAME),
    ?assertEqual([], ets:lookup(emqx_subseq, TestTopic)),
    emqx_broker_helper:create_seq(TestTopic),
    ?assertEqual([{TestTopic, 1}], ets:lookup(emqx_subseq, TestTopic)),
    emqx_broker_helper:reclaim_seq(TestTopic),
    ?assertEqual([], ets:lookup(emqx_subseq, TestTopic)).

t_shards_num(_) ->
    ?assertEqual(emqx_vm:schedulers() * 32, emqx_broker_helper:shards_num()).

t_get_sub_shard(_) ->
    ?assertEqual(0, emqx_broker_helper:get_sub_shard(self(), <<"topic">>)).

t_terminate(_) ->
    ?assertEqual(ok, gen_server:stop(emqx_broker_helper)).

t_uncovered_func(_) ->
    gen_server:call(emqx_broker_helper, test),
    gen_server:cast(emqx_broker_helper, test),
    emqx_broker_helper ! test.
