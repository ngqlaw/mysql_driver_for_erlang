%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_packet).

-include("mysql.hrl").

-export([
    decode/3
]).

%% @doc decode general packet
decode(<<Len:24/little, Index:8, Data/binary>>, Buff, Capability) ->
    {Payload, RestBin} = erlang:split_binary(Data, Len),
    NewPayload = <<Buff/binary, Payload/binary>>,
    Packet = case parser_gen_packet(NewPayload, Capability) of
        {error, need_more} ->
            #mysql_packet{sequence_id = Index, payload = false, buff = NewPayload};
        {error, unknown_packet_form} ->
            #mysql_packet{sequence_id = Index, payload = true, buff = NewPayload};
        Res ->
            #mysql_packet{sequence_id = Index, payload = Res}
    end,
    {Packet, RestBin}.

%% for general packet
parser_gen_packet(<<255:8, 255:8, 255:8, _Packet/binary>>, _Capability) ->
    {error, need_more};
parser_gen_packet(<<0:8, Packet/binary>>, Capability) ->
    ok_packet(Packet, Capability);
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
