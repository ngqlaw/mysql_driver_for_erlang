%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_handshake).

-include("mysql.hrl").

-export([
    v9/1,
    v10/1,
    auth_switch/1,
    auth_more/1
]).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc HandshakeV9
v9(Data) ->
    {_VersionDesc, <<ConnectID:32/little, Rest/binary>>} = mysql_util:parser_string_null(Data),
    {Scramble, _} = mysql_util:parser_string_null(Rest),
    #mysql_handshake{
        version = 9,
        connect_id = ConnectID,
        scramble = Scramble
    }.

%% @doc HandshakeV10
v10(Data) ->
    {_VersionDesc, <<ConnectID:32/little, Rest1/binary>>} = mysql_util:parser_string_null(Data),
    {AuthPluginDataPart1, <<
        0:8,
        CapabilityFlags1:16/little,
        CharacterSet:8,
        StatusFlags:16/little,
        CapabilityFlags2:16/little,
        TempAuthPluginDataLen:8,
        Rest2/binary
    >>} = erlang:split_binary(Rest1, 8),
    Capability = (CapabilityFlags2 bsl 16) + CapabilityFlags1,
    {_, Rest} = erlang:split_binary(Rest2, 10),
    AuthPluginDataLen =
        case ?CLIENT_PLUGIN_AUTH == (Capability band ?CLIENT_PLUGIN_AUTH) of
            true -> TempAuthPluginDataLen;
            false -> 0
        end,
    Bytes = max(13, AuthPluginDataLen - 8),
    {AuthPluginDataPart2, TempAuthPluginName} = erlang:split_binary(Rest, Bytes),
    case ?CLIENT_PLUGIN_AUTH == (Capability band ?CLIENT_PLUGIN_AUTH) of
        true ->
            {AuthPluginName, _} = mysql_util:parser_string_null(TempAuthPluginName);
        false ->
            AuthPluginName = <<>>
    end, 
    AuthPluginData = <<AuthPluginDataPart1/binary, AuthPluginDataPart2/binary>>,
    #mysql_handshake{
        version = 10,
        connect_id = ConnectID,
        scramble = AuthPluginData,
        capability = Capability,
        status_flags = StatusFlags,
        character_set = CharacterSet,
        auth_plugin_name = AuthPluginName
    }.

%% @doc AuthSwitchRequest
auth_switch(Data) ->
    {PluginName, PluginProvidedData} = mysql_util:parser_string_null(Data),
    #mysql_auth_switch{
        plugin_name = PluginName,
        data = PluginProvidedData
    }.

%% @doc AuthMoreData
auth_more(Data) ->
    #mysql_auth_more{data = Data}.
