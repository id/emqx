%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_maps).

-export([
    nested_get/2,
    nested_get/3,
    nested_put/3,
    range_gen/2,
    range_get/3
]).

-include_lib("emqx/include/emqx_placeholder.hrl").

nested_get(Key, Data) ->
    nested_get(Key, Data, undefined).

nested_get({var, ?PH_VAR_THIS}, Data, _Default) ->
    Data;
nested_get({var, Key}, Data, Default) ->
    general_map_get({key, Key}, Data, Data, Default);
nested_get({path, Path}, Data, Default) when is_list(Path) ->
    do_nested_get(Path, Data, Data, Default).

do_nested_get([Key | More], Data, OrgData, Default) ->
    case general_map_get(Key, Data, OrgData, undefined) of
        undefined -> Default;
        Val -> do_nested_get(More, Val, OrgData, Default)
    end;
do_nested_get([], Val, _OrgData, _Default) ->
    Val.

nested_put(Key, Val, Data) when
    not is_map(Data),
    not is_list(Data)
->
    nested_put(Key, Val, #{});
nested_put({var, Key}, Val, Map) ->
    general_map_put({key, Key}, Val, Map, Map);
nested_put({path, Path}, Val, Map) when is_list(Path) ->
    do_nested_put(Path, Val, Map, Map).

do_nested_put([Key | More], Val, Map, OrgData) ->
    SubMap = general_map_get(Key, Map, OrgData, undefined),
    general_map_put(Key, do_nested_put(More, Val, SubMap, OrgData), Map, OrgData);
do_nested_put([], Val, _Map, _OrgData) ->
    Val.

general_map_get(Key, Map, OrgData, Default) ->
    general_find(
        Key,
        Map,
        OrgData,
        fun
            ({equivalent, {_EquiKey, Val}}) -> Val;
            ({found, {_Key, Val}}) -> Val;
            (not_found) -> Default
        end
    ).

general_map_put(Key, Val, Map, OrgData) ->
    general_find(
        Key,
        Map,
        OrgData,
        fun
            ({equivalent, {EquiKey, _Val}}) -> do_put(EquiKey, Val, Map, OrgData);
            (_) -> do_put(Key, Val, Map, OrgData)
        end
    ).

general_find(KeyOrIndex, Data, OrgData, Handler) when is_binary(Data) ->
    try emqx_utils_json:decode(Data, [return_maps]) of
        Json -> general_find(KeyOrIndex, Json, OrgData, Handler)
    catch
        _:_ -> Handler(not_found)
    end;
general_find({key, Key}, Map, _OrgData, Handler) when is_map(Map) ->
    case Map of
        #{Key := Val} ->
            Handler({found, {{key, Key}, Val}});
        _ when is_atom(Key) ->
            %% the map may have an equivalent binary-form key
            BinKey = atom_to_binary(Key),
            case Map of
                #{BinKey := Val} -> Handler({equivalent, {{key, BinKey}, Val}});
                #{} -> Handler(not_found)
            end;
        _ when is_binary(Key) ->
            %% the map may have an equivalent atom-form key
            try
                AtomKey = list_to_existing_atom(binary_to_list(Key)),
                case Map of
                    #{AtomKey := Val} -> Handler({equivalent, {{key, AtomKey}, Val}});
                    #{} -> Handler(not_found)
                end
            catch
                error:badarg ->
                    Handler(not_found)
            end;
        _ ->
            Handler(not_found)
    end;
general_find({key, _Key}, _Map, _OrgData, Handler) ->
    Handler(not_found);
general_find({index, {const, Index0}} = IndexP, List, _OrgData, Handler) when is_list(List) ->
    handle_getnth(Index0, List, IndexP, Handler);
general_find({index, Index0} = IndexP, List, OrgData, Handler) when is_list(List) ->
    Index1 = nested_get(Index0, OrgData),
    handle_getnth(Index1, List, IndexP, Handler);
general_find({index, _}, List, _OrgData, Handler) when not is_list(List) ->
    Handler(not_found).

do_put({key, Key}, Val, Map, _OrgData) when is_map(Map) ->
    Map#{Key => Val};
do_put({key, Key}, Val, Data, _OrgData) when not is_map(Data) ->
    #{Key => Val};
do_put({index, {const, Index}}, Val, List, _OrgData) ->
    setnth(Index, List, Val);
do_put({index, Index0}, Val, List, OrgData) ->
    Index1 = nested_get(Index0, OrgData),
    setnth(Index1, List, Val).

setnth(_, Data, Val) when not is_list(Data) ->
    setnth(head, [], Val);
setnth(head, List, Val) when is_list(List) -> [Val | List];
setnth(head, _List, Val) ->
    [Val];
setnth(tail, List, Val) when is_list(List) -> List ++ [Val];
setnth(tail, _List, Val) ->
    [Val];
setnth(I, List, _Val) when not is_integer(I) -> List;
setnth(0, List, _Val) ->
    List;
setnth(I, List, Val) when is_integer(I), I > 0 ->
    do_setnth(I, List, Val);
setnth(I, List, Val) when is_integer(I), I < 0 ->
    lists:reverse(do_setnth(-I, lists:reverse(List), Val)).

do_setnth(1, [_ | Rest], Val) -> [Val | Rest];
do_setnth(I, [E | Rest], Val) -> [E | setnth(I - 1, Rest, Val)];
do_setnth(_, [], _Val) -> [].

getnth(0, _) ->
    {error, not_found};
getnth(I, L) when I > 0 ->
    do_getnth(I, L);
getnth(I, L) when I < 0 ->
    do_getnth(-I, lists:reverse(L)).

do_getnth(I, L) ->
    try
        {ok, lists:nth(I, L)}
    catch
        error:_ -> {error, not_found}
    end.

handle_getnth(Index, List, IndexPattern, Handler) ->
    case getnth(Index, List) of
        {ok, Val} ->
            Handler({found, {IndexPattern, Val}});
        {error, _} ->
            Handler(not_found)
    end.

range_gen(Begin, End) ->
    lists:seq(Begin, End).

range_get(Begin, End, List) when is_list(List) ->
    do_range_get(Begin, End, List);
range_get(_, _, _NotList) ->
    error({range_get, non_list_data}).

do_range_get(Begin, End, List) ->
    TotalLen = length(List),
    BeginIndex = index(Begin, TotalLen),
    EndIndex = index(End, TotalLen),
    lists:sublist(List, BeginIndex, (EndIndex - BeginIndex + 1)).

index(0, _) ->
    error({invalid_index, 0});
index(Index, _) when Index > 0 -> Index;
index(Index, Len) when Index < 0 ->
    Len + Index + 1.
