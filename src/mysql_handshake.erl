%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_handshake).

-include("mysql.hrl").

-export([
    decode/1,
    response/3
]).

%% @doc handshake start
decode(<<Len:24/little, Index:8, Data/binary>>) ->
    {Payload, _} = erlang:split_binary(Data, Len),
    parser(Payload, Index).

%% @doc handshake result
response(Data, Capability, #mysql_handle{packet = #mysql_packet{buff = Buff}} = Handle) ->
    case mysql_packet:decode(Data, Buff, Capability) of
        {#mysql_packet{sequence_id = Index, payload = true, buff = Payload}, _} ->
            % exchange further packets
            {further, parser(Payload, Index)};
        {#mysql_packet{payload = false} = NewPacket, _} ->
            {need_more, Handle#mysql_handle{packet = NewPacket}};
        {#mysql_packet{payload = #mysql_ok_packet{}}, _} ->
            ok;
        {#mysql_packet{payload = #mysql_err_packet{code = Code, message = Msg}}, _} ->
            {error, {Code, Msg}};
        {Error, _} ->
            {error, Error}
    end.

%% HandshakeV9
parser(<<9:8, Data/binary>>, Index) ->
    {_VersionDesc, <<ConnectID:32/little, Rest/binary>>} = mysql_util:parser_string_null(Data),
    {Scramble, _} = mysql_util:parser_string_null(Rest),
    #mysql_handshake_v9{
        connect_id = ConnectID,
        index = Index,
        scramble = Scramble
    };
%% HandshakeV10
parser(<<10:8, Data/binary>>, Index) ->
    {_VersionDesc, <<ConnectID:32/little, Rest1/binary>>} = mysql_util:parser_string_null(Data),
    {AuthPluginDataPart1, <<
        0:8,
        CapabilityFlags1:16/little,
        CharacterSet:8,
        StatusFlags:16/little,
        CapabilityFlags2:16/little,
        AuthPluginDataLen:8,
        Rest2/binary
    >>} = erlang:split_binary(Rest1, 8),
    {_, Rest} = erlang:split_binary(Rest2, 10),
    Bytes = max(13, AuthPluginDataLen - 8),
    {AuthPluginDataPart2, AuthPluginName} = erlang:split_binary(Rest, Bytes),
    Capability = (CapabilityFlags2 bsl 16) + CapabilityFlags1,
    AuthPluginData = <<AuthPluginDataPart1/binary, AuthPluginDataPart2/binary>>,
    #mysql_handshake_v10{
        connect_id = ConnectID,
        index = Index,
        scramble = AuthPluginData,
        capability = Capability,
        status_flags = StatusFlags,
        character_set = CharacterSet,
        auth_plugin_name = AuthPluginName
    };
%% AuthSwitchRequest
parser(<<254:8, Data/binary>>, Index) ->
    {PluginName, PluginProvidedData} = mysql_util:parser_string_null(Data),
    #mysql_auth_switch{
        index = Index,
        plugin_name = PluginName,
        data = PluginProvidedData
    };
%% AuthMoreData
parser(<<1:8, Data/binary>>, Index) ->
    #mysql_auth_more{index = Index, data = Data}.
