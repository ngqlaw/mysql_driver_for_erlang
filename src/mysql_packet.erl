%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_packet).

-include("mysql.hrl").

-export([
    handshake/1,
    packet/3,
    query_request/2,
    query_packet/3
]).

%% @doc handshake
handshake(<<_Len:24/little, Index:8, Data/binary>>) ->
    parser_handshake(Data, Index).

%% HandshakeV9
parser_handshake(<<9:8, Data/binary>>, Index) ->
    {_VersionDesc, <<ConnectID:32/little, Rest/binary>>} = mysql_util:parser_string_null(Data),
    {Scramble, _} = mysql_util:parser_string_null(Rest),
    #mysql_handshake_v9{
        connect_id = ConnectID,
        index = Index,
        scramble = Scramble
    };
%% HandshakeV10
parser_handshake(<<10:8, Data/binary>>, Index) ->
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
parser_handshake(<<254:8, Data/binary>>, Index) ->
    {PluginName, PluginProvidedData} = mysql_util:parser_string_null(Data),
    #mysql_auth_switch{
        index = Index,
        plugin_name = PluginName,
        data = PluginProvidedData
    };
%% AuthMoreData
parser_handshake(<<1:8, Data/binary>>, Index) ->
    #mysql_auth_more{index = Index, data = Data}.

%% @doc general packet
packet(<<Len:24/little, Index:8, Data/binary>>, Buff, Capability) ->
    {Payload, _RestBin} = erlang:split_binary(Data, Len),
    NewPayload = <<Buff/binary, Payload/binary>>,
    case parser_gen_packet(NewPayload, Capability) of
        {error, need_more} ->
            #mysql_packet{sequence_id = Index, payload = false, buff = NewPayload};
        {error, unknown_packet_form} ->
            #mysql_packet{sequence_id = Index, payload = true, buff = NewPayload};
        Res ->
            #mysql_packet{sequence_id = Index, payload = Res}
    end.

%% for general packet
parser_gen_packet(<<255:8, 255:8, 255:8, _Packet/binary>>, _Capability) ->
    {error, need_more};
parser_gen_packet(<<0:8, Packet/binary>>, Capability) ->
    ok_packet(Packet, Capability);
parser_gen_packet(<<251:8, Packet/binary>>, _Capability) ->
    {filename, Packet};
parser_gen_packet(<<255:8, Packet/binary>>, Capability) ->
    Protocal41Flags = ?CLIENT_PROTOCOL_41 band Capability,
    error_packet(Packet, Protocal41Flags);
parser_gen_packet(<<254:8, Packet/binary>>, Capability) when byte_size(Packet) < 8 ->
    Protocal41Flags = ?CLIENT_PROTOCOL_41 band Capability,
    eof_packet(Packet, Protocal41Flags);
parser_gen_packet(<<254:8, Packet/binary>>, Capability) ->
    ok_packet(Packet, Capability);
parser_gen_packet(_Packet, _Capability) ->
    {error, unknown_packet_form}.
    
%% OK_Packet
ok_packet(Packet, Capability) ->
    {AffectedRows, Rest1} = mysql_util:parser_integer(Packet),
    {LastInsertId, Rest2} = mysql_util:parser_integer(Rest1),
    if
        ?CLIENT_PROTOCOL_41 == (?CLIENT_PROTOCOL_41 band Capability) ->
            <<StatusFlags:16/little, Warnings:16/little, Rest3/binary>> = Rest2;
        ?CLIENT_TRANSACTIONS == (?CLIENT_TRANSACTIONS band Capability) ->
            <<StatusFlags:16/little, Rest3/binary>> = Rest2,
            Warnings = 0;
        true ->
            Rest3 = Rest2,
            StatusFlags = 0,
            Warnings = 0
    end,
    case ?CLIENT_SESSION_TRACK == (?CLIENT_SESSION_TRACK band Capability) of
        true ->
            {Info, Rest4} = mysql_util:parser_string_lenenc(Rest3);
        false ->
            Info = <<>>,
            Rest4 = Rest3
    end,
    case ?SERVER_SESSION_STATE_CHANGED == (?SERVER_SESSION_STATE_CHANGED band StatusFlags) of
        true ->
            {SessionStateChangesBin, _Rest5} = mysql_util:parser_string_lenenc(Rest4),
            SessionStateChanges = parser_session_state_changes(SessionStateChangesBin, []),
            NewInfo = Info;
        false ->
            SessionStateChanges = [],
            NewInfo = Rest4
    end,
    #mysql_ok_packet{
        affected_rows = AffectedRows,
        last_insert_id = LastInsertId,
        status_flags = StatusFlags,
        warnings = Warnings,
        session_state_info = SessionStateChanges,
        info = NewInfo
    }.

parser_session_state_changes(<<Type:8, Rest/binary>>, Result) ->
    {B, NewRest} = mysql_util:parser_string_lenenc(Rest),
    S = do_parser_session_state_change(Type, B, []),
    parser_session_state_changes(NewRest, [{Type, S}|Result]);
parser_session_state_changes(<<>>, Result) ->
    lists:reverse(Result).

do_parser_session_state_change(_Type, <<>>, Result) ->
    lists:reverse(Result);
do_parser_session_state_change(?SESSION_TRACK_SYSTEM_VARIABLES, Bin, Result) ->
    {Name, Bin1} = mysql_util:parser_string_lenenc(Bin),
    {Value, Bin2} = mysql_util:parser_string_lenenc(Bin1),
    do_parser_session_state_change(?SESSION_TRACK_SYSTEM_VARIABLES, Bin2, [{Name, Value}|Result]);
do_parser_session_state_change(?SESSION_TRACK_SCHEMA, Bin, Result) ->
    {Name, Bin1} = mysql_util:parser_string_lenenc(Bin),
    do_parser_session_state_change(?SESSION_TRACK_SCHEMA, Bin1, [Name|Result]);
do_parser_session_state_change(?SESSION_TRACK_STATE_CHANGE, Bin, Result) ->
    {IsTracked, Bin1} = mysql_util:parser_string_lenenc(Bin),
    do_parser_session_state_change(?SESSION_TRACK_STATE_CHANGE, Bin1, [IsTracked|Result]).

%% EOF Packet
eof_packet(<<Warnings:16/little, StatusFlags:16/little>>, ?CLIENT_PROTOCOL_41) ->
    #mysql_eof_packet{warnings = Warnings, status_flags = StatusFlags};
eof_packet(<<>>, 0) ->
    #mysql_eof_packet{warnings = 0, status_flags = 0}.

%% ERR_Packet
error_packet(<<Code:16/little, SqlStateMarker:8, Bin/binary>>, ?CLIENT_PROTOCOL_41) ->
    {SqlState, Message} = erlang:split_binary(Bin, 5),
    #mysql_err_packet{
        code = Code,
        sql_state_marker = <<SqlStateMarker:8>>,
        sql_state = SqlState,
        message = Message
    };
error_packet(<<Code:16/little, Message/binary>>, 0) ->
    #mysql_err_packet{
        code = Code,
        message = Message
    }.

%% @doc 发送请求
query_request(String, Index) when is_list(String) ->
    Bin = iolist_to_binary(String),
    query_request(Bin, Index);
query_request(Bin, Index) when is_binary(Bin) ->
    Len = byte_size(Bin) + 1,
    <<Len:24/little, Index:8, ?COM_QUERY:8, Bin/binary>>.

%% @doc for query result
query_packet(Packet, _Capability, 0) ->
    case mysql_util:parser_integer(Packet) of
        {251, <<>>} ->
            {column_count, 251};
        {251, Rest} -> %for now is ignore
            {?CLIENT_LOCAL_FILES, Rest};
        {ColumnCount, <<>>} ->
            {column_count, ColumnCount};
        {_, _} ->
            {error, unknown_packet_form}
    end;
query_packet(Packet, Capability, 1) ->
    ColumnDefinition =
    case ?CLIENT_PROTOCOL_41 == Capability band ?CLIENT_PROTOCOL_41 of
        true ->
            column_definition_41(Packet, Capability, ?COM_QUERY);
        false ->
            column_definition_320(Packet, Capability, ?COM_QUERY)
    end,
    {column_definition, ColumnDefinition};
query_packet(<<251:8, _Bin/binary>>, _Capability, 3) ->
    {row, []};
query_packet(Packet, _Capability, 3) ->
    RowInfo = mysql_util:parser_string_lenencs(Packet),
    {row, RowInfo}.

%% Protocol::ColumnDefinition41
%% lenenc_str     catalog
%% lenenc_str     schema
%% lenenc_str     table
%% lenenc_str     org_table
%% lenenc_str     name
%% lenenc_str     org_name
%% lenenc_int     length of fixed-length fields [0c]
%% 2              character set
%% 4              column length
%% 1              type
%% 2              flags
%% 1              decimals
%% 2              filler [00] [00]
%%   if command was COM_FIELD_LIST {
%% lenenc_int     length of default-values
%% string[$len]   default values
%%   }
column_definition_41(Bin, _Capability, COM) ->
    {Catalog, Rest1} = mysql_util:parser_string_lenenc(Bin),
    {Schema, Rest2} = mysql_util:parser_string_lenenc(Rest1),
    {Table, Rest3} = mysql_util:parser_string_lenenc(Rest2),
    {OrgTable, Rest4} = mysql_util:parser_string_lenenc(Rest3),
    {Name, Rest5} = mysql_util:parser_string_lenenc(Rest4),
    {OrgName, Rest6} = mysql_util:parser_string_lenenc(Rest5),
    {_NextLength, 
    <<
        CharacterSet:16, 
        ColumnLength:32, 
        Type:8, 
        Flags:16, 
        Decimals:8, 
        _Filler:16, 
        Rest7/binary
    >>} = mysql_util:parser_integer(Rest6),
    case COM == ?COM_FIELD_LIST of
        true ->
            {DefaultValues, _Rest8} = mysql_util:parser_string_lenenc(Rest7);
        false ->
            DefaultValues = <<>>
    end,
    [Catalog, Schema, Table, OrgTable, Name, OrgName, CharacterSet, ColumnLength, Type, Flags, Decimals, DefaultValues].

%% Protocol::ColumnDefinition320
%% lenenc-str     table
%% lenenc-str     name
%% lenenc_int     [03] length of the column_length field
%% 3              column_length
%% lenenc_int     [01] length of type field
%% 1              type
%%   if capabilities & CLIENT_LONG_FLAG {
%% lenenc_int     [03] length of flags+decimals fields
%% 2              flags
%% 1              decimals
%%   } else {
%% 1              [02] length of flags+decimals fields
%% 1              flags
%% 1              decimals
%%   }
%%   if command was COM_FIELD_LIST {
%% lenenc_int     length of default-values
%% string[$len]   default values
%%   }
column_definition_320(Bin, Capability, COM) ->
    {Table, Rest1} = mysql_util:parser_string_lenenc(Bin),
    {Name, Rest2} = mysql_util:parser_string_lenenc(Rest1),
    {_NextLength, <<ColumnLength:24, Rest3/binary>>} = mysql_util:parser_integer(Rest2),
    {_LengthOfTypeField, <<Type:8, Rest4/binary>>} = mysql_util:parser_integer(Rest3),
    case ?CLIENT_LONG_FLAG == ?CLIENT_LONG_FLAG band Capability of
        true ->
            {_Len1, <<Flags:16, Decimals:8, Rest5/binary>>} = mysql_util:parser_integer(Rest4);
        false ->
            <<_Len1:8, Flags:8, Decimals:8, Rest5/binary>> = Rest4
    end,
    case COM == ?COM_FIELD_LIST of
        true ->
            {DefaultValues, _Rest6} = mysql_util:parser_string_lenenc(Rest5);
        false ->
            DefaultValues = <<>>
    end,
    [Table, Name, ColumnLength, Type, Flags, Decimals, DefaultValues].
