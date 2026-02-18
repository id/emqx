-module(emqx_schema_validation_topic_tree).

-include_lib("emqx/include/logger.hrl").

%% API
-export([
    load/1,
    compile/1,
    validate/2
]).

-export_type([
    compiled_model/0,
    validation_result/0
]).

-type compiled_node() :: #{
    type := namespace | variable | endpoint,
    description := binary(),
    %% namespace / variable nodes
    ns_children => #{binary() => compiled_node()},
    var_children => [{binary(), compiled_node()}],
    %% variable nodes
    var_name => binary(),
    validator => fun((binary()) -> boolean()),
    %% endpoint nodes
    payload_type => binary(),
    payload_schema => map() | undefined
}.

-type compiled_model() :: #{
    tree := #{binary() => compiled_node()},
    settings := map(),
    on_mismatch := drop | disconnect | log_only,
    exempt_patterns := [emqx_types:words()],
    variable_types := map(),
    payload_types := map()
}.

-type validation_result() ::
    ok
    | {error, topic_invalid, binary()}
    | {error, not_endpoint, binary()}
    | {error, payload_invalid, binary()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(map()) -> {ok, compiled_model()} | {error, term()}.
compile(DataModel) when is_map(DataModel) ->
    parse_data_model(DataModel).

-spec load(file:name_all()) -> {ok, compiled_model()} | {error, term()}.
load(Filename) ->
    case file:read_file(Filename) of
        {ok, Content} ->
            case emqx_utils_json:safe_decode(Content, [return_maps]) of
                {ok, Data} ->
                    parse_data_model(Data);
                {error, Reason} ->
                    {error, {json_decode_error, Reason}}
            end;
        {error, Reason} ->
            {error, {read_file_error, Reason}}
    end.

-spec validate(compiled_model(), emqx_types:message()) -> validation_result().
validate(Model, Message) ->
    Topic = emqx_message:topic(Message),
    Payload = emqx_message:payload(Message),
    #{exempt_patterns := ExemptPatterns, tree := Tree} = Model,
    Words = emqx_topic:words(Topic),
    case is_exempt(Words, ExemptPatterns) of
        true ->
            ok;
        false ->
            do_validate(Tree, Words, Payload)
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

parse_data_model(Data) ->
    case maps:get(<<"tree">>, Data, undefined) of
        undefined ->
            {error, missing_tree};
        Tree ->
            VarTypes = maps:get(<<"variable_types">>, Data, #{}),
            PayloadTypes = maps:get(<<"payload_types">>, Data, #{}),
            Settings = maps:get(<<"settings">>, Data, #{}),

            case validate_and_compile_tree(Tree, VarTypes, PayloadTypes) of
                {ok, CompiledTree} ->
                    OnMismatch = parse_on_mismatch(
                        maps:get(<<"on_mismatch">>, Settings, <<"drop">>)
                    ),
                    ExemptPatterns = parse_exempt_topics(
                        maps:get(<<"exempt_topics">>, Settings, [])
                    ),
                    {ok, #{
                        tree => CompiledTree,
                        settings => Settings,
                        on_mismatch => OnMismatch,
                        exempt_patterns => ExemptPatterns,
                        variable_types => VarTypes,
                        payload_types => PayloadTypes
                    }};
                {error, _} = Error ->
                    Error
            end
    end.

parse_on_mismatch(<<"disconnect">>) -> disconnect;
parse_on_mismatch(<<"log_only">>) -> log_only;
parse_on_mismatch(_) -> drop.

parse_exempt_topics(Topics) when is_list(Topics) ->
    [emqx_topic:words(T) || T <- Topics];
parse_exempt_topics(_) ->
    [].

is_exempt(_Words, []) ->
    false;
is_exempt(Words, [Pattern | Rest]) ->
    case emqx_topic:match(Words, Pattern) of
        true -> true;
        false -> is_exempt(Words, Rest)
    end.

%%------------------------------------------------------------------------------
%% Validation + Compilation
%%------------------------------------------------------------------------------

validate_and_compile_tree(Nodes, VarTypes, PayloadTypes) ->
    try
        CompiledTree = compile_tree(Nodes, VarTypes, PayloadTypes, [<<"tree">>]),
        {ok, CompiledTree}
    catch
        throw:{compilation_error, Path, Message} ->
            {error, {compilation_error, Path, Message}}
    end.

compile_tree(Nodes, VarTypes, PayloadTypes, Path) when is_map(Nodes) ->
    maps:fold(
        fun(Key, NodeData, #{ns := NsAcc, var := VarAcc}) ->
            NodePath = Path ++ [Key],
            CompiledNode = compile_node(Key, NodeData, VarTypes, PayloadTypes, NodePath),
            case maps:get(type, CompiledNode) of
                variable ->
                    #{ns => NsAcc, var => VarAcc ++ [{Key, CompiledNode}]};
                _ ->
                    #{ns => NsAcc#{Key => CompiledNode}, var => VarAcc}
            end
        end,
        #{ns => #{}, var => []},
        Nodes
    );
compile_tree(_Nodes, _VarTypes, _PayloadTypes, _Path) ->
    #{ns => #{}, var => []}.

compile_node(Key, NodeData, VarTypes, PayloadTypes, Path) ->
    Type = maps:get(<<"_type">>, NodeData, undefined),
    validate_node_type(Type, Path),
    Children = maps:get(<<"children">>, NodeData, undefined),

    BaseNode = #{
        type => binary_to_atom(Type, utf8),
        description => maps:get(<<"_description">>, NodeData, <<>>)
    },

    case Type of
        <<"namespace">> ->
            validate_key_not_variable(Key, Path),
            validate_has_children(Children, Path),
            #{ns := NsChildren, var := VarChildren} =
                compile_tree(Children, VarTypes, PayloadTypes, Path ++ [<<"children">>]),
            BaseNode#{
                ns_children => NsChildren,
                var_children => VarChildren
            };
        <<"variable">> ->
            validate_key_is_variable(Key, Path),
            validate_has_children(Children, Path),
            VarTypeName = maps:get(<<"_var_type">>, NodeData, undefined),
            validate_var_type_exists(VarTypeName, VarTypes, Path),
            VarTypeDef = maps:get(VarTypeName, VarTypes, undefined),
            Validator = compile_var_validator(VarTypeDef),
            #{ns := NsChildren, var := VarChildren} =
                compile_tree(Children, VarTypes, PayloadTypes, Path ++ [<<"children">>]),
            BaseNode#{
                var_name => Key,
                validator => Validator,
                ns_children => NsChildren,
                var_children => VarChildren
            };
        <<"endpoint">> ->
            validate_no_children(Children, Path),
            PayloadTypeName = maps:get(<<"_payload">>, NodeData, <<"any">>),
            validate_payload_type_exists(PayloadTypeName, PayloadTypes, Path),
            BaseNode#{
                payload_type => PayloadTypeName,
                payload_schema => maps:get(PayloadTypeName, PayloadTypes, undefined)
            }
    end.

%%------------------------------------------------------------------------------
%% Data model validation helpers
%%------------------------------------------------------------------------------

validate_node_type(<<"namespace">>, _Path) ->
    ok;
validate_node_type(<<"variable">>, _Path) ->
    ok;
validate_node_type(<<"endpoint">>, _Path) ->
    ok;
validate_node_type(undefined, Path) ->
    throw({compilation_error, Path, <<"missing _type field">>});
validate_node_type(Other, Path) ->
    throw(
        {compilation_error, Path,
            iolist_to_binary([<<"invalid _type: ">>, io_lib:format("~p", [Other])])}
    ).

validate_key_not_variable(Key, Path) ->
    case is_variable_key(Key) of
        true ->
            throw({compilation_error, Path, <<"namespace key must not use {variable} syntax">>});
        false ->
            ok
    end.

validate_key_is_variable(Key, Path) ->
    case is_variable_key(Key) of
        false ->
            throw({compilation_error, Path, <<"variable key must use {variable} syntax">>});
        true ->
            ok
    end.

validate_has_children(undefined, Path) ->
    throw({compilation_error, Path, <<"namespace/variable node must have non-empty children">>});
validate_has_children(Children, Path) when is_map(Children) ->
    case map_size(Children) of
        0 ->
            throw(
                {compilation_error, Path,
                    <<"namespace/variable node must have non-empty children">>}
            );
        _ ->
            ok
    end;
validate_has_children(_, Path) ->
    throw({compilation_error, Path, <<"children must be a map">>}).

validate_no_children(undefined, _Path) ->
    ok;
validate_no_children(Children, _Path) when is_map(Children), map_size(Children) =:= 0 -> ok;
validate_no_children(_, Path) ->
    throw({compilation_error, Path, <<"endpoint node must not have children">>}).

validate_var_type_exists(undefined, _VarTypes, Path) ->
    throw({compilation_error, Path, <<"variable node missing _var_type">>});
validate_var_type_exists(Name, VarTypes, Path) ->
    case maps:is_key(Name, VarTypes) of
        true ->
            ok;
        false ->
            throw(
                {compilation_error, Path, iolist_to_binary([<<"unknown variable type: ">>, Name])}
            )
    end.

validate_payload_type_exists(<<"any">>, _PayloadTypes, _Path) ->
    ok;
validate_payload_type_exists(Name, PayloadTypes, Path) ->
    case maps:is_key(Name, PayloadTypes) of
        true ->
            ok;
        false ->
            throw({compilation_error, Path, iolist_to_binary([<<"unknown payload type: ">>, Name])})
    end.

is_variable_key(<<"{", Rest/binary>>) ->
    byte_size(Rest) > 1 andalso binary:last(Rest) =:= $};
is_variable_key(_) ->
    false.

%%------------------------------------------------------------------------------
%% Variable validator compilation
%%------------------------------------------------------------------------------

compile_var_validator(undefined) ->
    fun(_) -> true end;
compile_var_validator(#{<<"type">> := <<"string">>, <<"pattern">> := Pattern}) ->
    {ok, MP} = re:compile(Pattern),
    fun(Value) ->
        case re:run(Value, MP) of
            {match, _} -> true;
            _ -> false
        end
    end;
compile_var_validator(#{<<"type">> := <<"enum">>, <<"values">> := Values}) ->
    ValueSet = sets:from_list(Values, [{version, 2}]),
    fun(Value) -> sets:is_element(Value, ValueSet) end;
compile_var_validator(_) ->
    fun(_) -> true end.

%%------------------------------------------------------------------------------
%% Topic validation (tree traversal)
%%------------------------------------------------------------------------------

do_validate(Tree, Tokens, Payload) ->
    match_segment(Tree, Tokens, Payload, #{}).

%% Tree here is #{ns := ..., var := ...} for internal nodes,
%% or #{Key => compiled_node()} for root level.
%% Root level is the top-level compiled tree which uses the split format.
match_segment(#{ns := NsChildren, var := VarChildren}, [Token | Rest], Payload, CapturedVars) ->
    match_segment_split(NsChildren, VarChildren, Token, Rest, Payload, CapturedVars);
match_segment(#{ns := _, var := _}, [], _Payload, _CapturedVars) ->
    {error, not_endpoint, <<"topic too short, landed on intermediate node">>};
match_segment(_Tree, _Tokens, _Payload, _CapturedVars) ->
    {error, topic_invalid, <<"empty or invalid tree">>}.

match_segment_split(NsChildren, VarChildren, Token, Rest, Payload, CapturedVars) ->
    %% 1. Try exact namespace match (O(1))
    case maps:find(Token, NsChildren) of
        {ok, Node} ->
            proceed(Node, Token, Rest, Payload, CapturedVars);
        error ->
            %% 2. Try variable matches (linear scan, typically 0-1 entries)
            find_variable_match(VarChildren, Token, Rest, Payload, CapturedVars)
    end.

find_variable_match([], Token, _Rest, _Payload, _CapturedVars) ->
    {error, topic_invalid, iolist_to_binary([<<"no matching segment for: ">>, Token])};
find_variable_match([{Key, Node} | Tail], Token, Rest, Payload, CapturedVars) ->
    #{validator := Validator} = Node,
    case Validator(Token) of
        true ->
            VarName = extract_var_name(Key),
            NewCaptured = CapturedVars#{VarName => Token},
            case proceed(Node, Token, Rest, Payload, NewCaptured) of
                ok ->
                    ok;
                {error, topic_invalid, _} = _TopicErr ->
                    %% Only retry next variable on topic_invalid (wrong branch).
                    %% not_endpoint and payload_invalid mean the path matched
                    %% but validation failed — don't try other variables.
                    find_variable_match(Tail, Token, Rest, Payload, CapturedVars);
                Other ->
                    Other
            end;
        false ->
            find_variable_match(Tail, Token, Rest, Payload, CapturedVars)
    end.

extract_var_name(Bin) ->
    %% Strip { and }
    S = byte_size(Bin) - 2,
    <<_, Name:S/binary, _>> = Bin,
    Name.

proceed(Node, _Token, [], Payload, CapturedVars) ->
    %% End of topic — node must be endpoint
    case maps:get(type, Node) of
        endpoint ->
            validate_payload(Node, Payload, CapturedVars);
        _ ->
            {error, not_endpoint, <<"topic too short, landed on intermediate node">>}
    end;
proceed(Node, _Token, [_ | _] = Rest, Payload, CapturedVars) ->
    %% More tokens — node must NOT be endpoint
    case maps:get(type, Node) of
        endpoint ->
            {error, topic_invalid, <<"topic has extra segments past endpoint">>};
        _ ->
            NsChildren = maps:get(ns_children, Node, #{}),
            VarChildren = maps:get(var_children, Node, []),
            match_segment(
                #{ns => NsChildren, var => VarChildren},
                Rest,
                Payload,
                CapturedVars
            )
    end.

%%------------------------------------------------------------------------------
%% Payload validation
%%------------------------------------------------------------------------------

validate_payload(#{payload_type := <<"any">>}, _Payload, _CapturedVars) ->
    ok;
validate_payload(#{payload_schema := Schema}, Payload, _CapturedVars) when
    Schema =/= undefined
->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {ok, Decoded} ->
            try jesse:validate_with_schema(Schema, Decoded) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    {error, payload_invalid, iolist_to_binary(io_lib:format("~p", [Reason]))}
            catch
                _:E ->
                    {error, payload_invalid,
                        iolist_to_binary(io_lib:format("schema validation error: ~p", [E]))}
            end;
        {error, _} ->
            {error, payload_invalid, <<"payload is not valid JSON">>}
    end;
validate_payload(_, _Payload, _CapturedVars) ->
    ok.
