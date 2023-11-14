%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

bridge_v1_type_name() -> syskeeper_forwarder.

action_type_name() -> syskeeper_forwarder.

connector_type_name() -> syskeeper_forwarder.

schema_module() -> emqx_bridge_syskeeper.
