%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_topic_tree_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Test helpers
%%------------------------------------------------------------------------------

make_message(Topic, Payload) ->
    emqx_message:make(<<"test_client">>, Topic, Payload).

sample_data_model() ->
    #{
        <<"version">> => <<"1.0">>,
        <<"variable_types">> => #{
            <<"site_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Z]{2}\\d{3}$">>
            },
            <<"device_type">> => #{
                <<"type">> => <<"enum">>,
                <<"values">> => [<<"sensor">>, <<"actuator">>, <<"gateway">>]
            }
        },
        <<"payload_types">> => #{
            <<"temperature_reading">> => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"temperature">> => #{<<"type">> => <<"number">>},
                    <<"unit">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"celsius">>, <<"fahrenheit">>]
                    }
                },
                <<"required">> => [<<"temperature">>, <<"unit">>],
                <<"additionalProperties">> => false
            }
        },
        <<"settings">> => #{
            <<"on_mismatch">> => <<"drop">>,
            <<"exempt_topics">> => [<<"$SYS/#">>, <<"$share/#">>]
        },
        <<"tree">> => #{
            <<"factory">> => #{
                <<"_type">> => <<"namespace">>,
                <<"_description">> => <<"Factory namespace">>,
                <<"children">> => #{
                    <<"{site_id}">> => #{
                        <<"_type">> => <<"variable">>,
                        <<"_var_type">> => <<"site_id">>,
                        <<"_description">> => <<"Site identifier">>,
                        <<"children">> => #{
                            <<"{device_type}">> => #{
                                <<"_type">> => <<"variable">>,
                                <<"_var_type">> => <<"device_type">>,
                                <<"_description">> => <<"Device type">>,
                                <<"children">> => #{
                                    <<"temperature">> => #{
                                        <<"_type">> => <<"endpoint">>,
                                        <<"_payload">> => <<"temperature_reading">>,
                                        <<"_description">> => <<"Temperature data">>
                                    },
                                    <<"status">> => #{
                                        <<"_type">> => <<"endpoint">>,
                                        <<"_payload">> => <<"any">>,
                                        <<"_description">> => <<"Device status">>
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }.

load_model(DataModel) ->
    TmpFile = "/tmp/emqx_topic_tree_test.json",
    Content = emqx_utils_json:encode(DataModel),
    ok = file:write_file(TmpFile, Content),
    Result = emqx_schema_validation_topic_tree:load(TmpFile),
    file:delete(TmpFile),
    Result.

%%------------------------------------------------------------------------------
%% Valid path tests
%%------------------------------------------------------------------------------

valid_full_path_endpoint_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(
        <<"factory/AB123/sensor/temperature">>,
        <<"{\"temperature\": 22.5, \"unit\": \"celsius\"}">>
    ),
    ?assertEqual(ok, emqx_schema_validation_topic_tree:validate(Model, Msg)).

valid_path_any_payload_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"factory/AB123/actuator/status">>, <<"anything goes">>),
    ?assertEqual(ok, emqx_schema_validation_topic_tree:validate(Model, Msg)).

%%------------------------------------------------------------------------------
%% Invalid segment tests
%%------------------------------------------------------------------------------

unknown_first_segment_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"unknown/AB123/sensor/temperature">>, <<>>),
    {error, topic_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

unknown_last_segment_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"factory/AB123/sensor/nonexistent">>, <<>>),
    {error, topic_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

%%------------------------------------------------------------------------------
%% Too short / too long tests
%%------------------------------------------------------------------------------

topic_too_short_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"factory/AB123">>, <<>>),
    {error, not_endpoint, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

topic_too_short_namespace_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"factory">>, <<>>),
    {error, not_endpoint, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

topic_too_long_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"factory/AB123/sensor/temperature/extra">>, <<>>),
    {error, topic_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

%%------------------------------------------------------------------------------
%% Variable constraint tests
%%------------------------------------------------------------------------------

variable_regex_fail_test() ->
    {ok, Model} = load_model(sample_data_model()),
    %% site_id must match ^[A-Z]{2}\d{3}$
    Msg = make_message(
        <<"factory/invalid/sensor/temperature">>,
        <<"{\"temperature\": 22.5, \"unit\": \"celsius\"}">>
    ),
    {error, topic_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

variable_enum_fail_test() ->
    {ok, Model} = load_model(sample_data_model()),
    %% device_type must be sensor, actuator, or gateway
    Msg = make_message(
        <<"factory/AB123/unknown_type/temperature">>,
        <<"{\"temperature\": 22.5, \"unit\": \"celsius\"}">>
    ),
    {error, topic_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

variable_regex_pass_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"factory/ZZ999/gateway/status">>, <<"ok">>),
    ?assertEqual(ok, emqx_schema_validation_topic_tree:validate(Model, Msg)).

%%------------------------------------------------------------------------------
%% Exempt topic tests
%%------------------------------------------------------------------------------

exempt_sys_topic_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"$SYS/broker/uptime">>, <<>>),
    ?assertEqual(ok, emqx_schema_validation_topic_tree:validate(Model, Msg)).

exempt_share_topic_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"$share/group1/some/topic">>, <<>>),
    ?assertEqual(ok, emqx_schema_validation_topic_tree:validate(Model, Msg)).

non_exempt_topic_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(<<"not_exempt/something">>, <<>>),
    {error, topic_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

%%------------------------------------------------------------------------------
%% Payload validation tests
%%------------------------------------------------------------------------------

payload_valid_json_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(
        <<"factory/AB123/sensor/temperature">>,
        <<"{\"temperature\": 22.5, \"unit\": \"celsius\"}">>
    ),
    ?assertEqual(ok, emqx_schema_validation_topic_tree:validate(Model, Msg)).

payload_missing_required_field_test() ->
    {ok, Model} = load_model(sample_data_model()),
    %% Missing "unit" field
    Msg = make_message(
        <<"factory/AB123/sensor/temperature">>,
        <<"{\"temperature\": 22.5}">>
    ),
    {error, payload_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

payload_wrong_enum_value_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(
        <<"factory/AB123/sensor/temperature">>,
        <<"{\"temperature\": 22.5, \"unit\": \"kelvin\"}">>
    ),
    {error, payload_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

payload_extra_fields_test() ->
    {ok, Model} = load_model(sample_data_model()),
    %% additionalProperties is false
    Msg = make_message(
        <<"factory/AB123/sensor/temperature">>,
        <<"{\"temperature\": 22.5, \"unit\": \"celsius\", \"extra\": 1}">>
    ),
    {error, payload_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

payload_not_json_test() ->
    {ok, Model} = load_model(sample_data_model()),
    Msg = make_message(
        <<"factory/AB123/sensor/temperature">>,
        <<"not json at all">>
    ),
    {error, payload_invalid, _} = emqx_schema_validation_topic_tree:validate(Model, Msg).

%%------------------------------------------------------------------------------
%% Load-time validation error tests
%%------------------------------------------------------------------------------

missing_var_type_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"{bad_var}">> => #{
                <<"_type">> => <<"variable">>,
                <<"_var_type">> => <<"nonexistent">>,
                <<"children">> => #{
                    <<"ep">> => #{
                        <<"_type">> => <<"endpoint">>
                    }
                }
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

endpoint_with_children_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ns">> => #{
                <<"_type">> => <<"endpoint">>,
                <<"children">> => #{
                    <<"child">> => #{
                        <<"_type">> => <<"endpoint">>
                    }
                }
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

namespace_without_children_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ns">> => #{
                <<"_type">> => <<"namespace">>
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

namespace_with_empty_children_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ns">> => #{
                <<"_type">> => <<"namespace">>,
                <<"children">> => #{}
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

invalid_type_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ns">> => #{
                <<"_type">> => <<"invalid_type">>
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

missing_type_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ns">> => #{
                <<"_description">> => <<"no type">>
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

namespace_key_with_variable_syntax_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"{bad_ns}">> => #{
                <<"_type">> => <<"namespace">>,
                <<"children">> => #{
                    <<"ep">> => #{
                        <<"_type">> => <<"endpoint">>
                    }
                }
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

variable_key_without_braces_test() ->
    DataModel = #{
        <<"variable_types">> => #{
            <<"my_var">> => #{<<"type">> => <<"enum">>, <<"values">> => [<<"a">>]}
        },
        <<"tree">> => #{
            <<"bad_variable">> => #{
                <<"_type">> => <<"variable">>,
                <<"_var_type">> => <<"my_var">>,
                <<"children">> => #{
                    <<"ep">> => #{
                        <<"_type">> => <<"endpoint">>
                    }
                }
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

unknown_payload_type_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ep">> => #{
                <<"_type">> => <<"endpoint">>,
                <<"_payload">> => <<"nonexistent_schema">>
            }
        }
    },
    {error, {compilation_error, _, _}} = load_model(DataModel).

%%------------------------------------------------------------------------------
%% Settings tests
%%------------------------------------------------------------------------------

on_mismatch_default_test() ->
    DataModel = #{
        <<"tree">> => #{
            <<"ep">> => #{<<"_type">> => <<"endpoint">>}
        }
    },
    {ok, Model} = load_model(DataModel),
    ?assertEqual(drop, maps:get(on_mismatch, Model)).

on_mismatch_disconnect_test() ->
    DataModel = #{
        <<"settings">> => #{<<"on_mismatch">> => <<"disconnect">>},
        <<"tree">> => #{
            <<"ep">> => #{<<"_type">> => <<"endpoint">>}
        }
    },
    {ok, Model} = load_model(DataModel),
    ?assertEqual(disconnect, maps:get(on_mismatch, Model)).

on_mismatch_log_only_test() ->
    DataModel = #{
        <<"settings">> => #{<<"on_mismatch">> => <<"log_only">>},
        <<"tree">> => #{
            <<"ep">> => #{<<"_type">> => <<"endpoint">>}
        }
    },
    {ok, Model} = load_model(DataModel),
    ?assertEqual(log_only, maps:get(on_mismatch, Model)).

%%------------------------------------------------------------------------------
%% Missing tree test
%%------------------------------------------------------------------------------

missing_tree_test() ->
    DataModel = #{<<"version">> => <<"1.0">>},
    {error, missing_tree} = load_model(DataModel).

%%------------------------------------------------------------------------------
%% File error tests
%%------------------------------------------------------------------------------

file_not_found_test() ->
    {error, {read_file_error, enoent}} =
        emqx_schema_validation_topic_tree:load("/tmp/nonexistent_file.json").
