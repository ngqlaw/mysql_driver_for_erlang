%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_util).

-include("mysql.hrl").

%% for Old Password Authentication
-define(CODE_N1, 	1345345333).
-define(CODE_N2, 	305419889).
-define(CODE_SUM, 	7).
-define(MASK_30, 	1073741823).
-define(MASK_31, 	2147483647).

-export([
		parser_integer/1,
		parser_string_null/1,
		parser_string_lenenc/1,
		hash/2,
		hash_old/2,
		bxor_binary/2,
		query_request/2,
		parser_handshake/2,
		parser_query/3,
		parser_result/1
		]).

parser_integer(<<252:8, Integer:16/little, Rest/binary>>) ->
	{Integer, Rest};
parser_integer(<<253:8, Integer:24/little, Rest/binary>>) ->
	{Integer, Rest};
parser_integer(<<254:8, Integer:64/little, Rest/binary>>) ->
	{Integer, Rest};
parser_integer(<<254:8, Rest/binary>>) when byte_size(Rest) < 8 ->
	eof;
parser_integer(<<Integer:8, Rest/binary>>) ->
	{Integer, Rest}.
	
parser_string_null(Bin) ->
	parser_string_null(Bin, <<>>).
parser_string_null(<<I:8,Rest/binary>>, Result) when I > 0 ->
	parser_string_null(Rest, <<Result/binary, I:8>>);
parser_string_null(<<_:8, Bin/binary>>, Result) ->
	{Result, Bin}.
	
parser_string_lenenc(Bin) ->
	{Len, Rest} = parser_integer(Bin),
	erlang:split_binary(Rest, Len).


parser_string_lenencs(<<>>, Acc) ->
	Acc;
parser_string_lenencs(Bin, Acc) ->
	{Value, Rest} = parser_string_lenenc(Bin),
	parser_string_lenencs(Rest, [Value|Acc]).
	
%% SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
hash(AuthPluginData, Password) ->
	Pwd = crypto:hash(sha, Password),
	Pwd1 = crypto:hash(sha, Pwd),
	Context1 = crypto:hash_init(sha),
	Context2 = crypto:hash_update(Context1, AuthPluginData),
	Context3 = crypto:hash_update(Context2, Pwd1),
	Pwd2 = crypto:hash_final(Context3),
	bxor_binary(Pwd, Pwd2).
	
%% [WARNING]Algorithm-MYSQL323 broken!
hash_old(AuthPluginData, Password) when is_list(AuthPluginData) andalso is_list(Password) ->
    {P1, P2} = do_hash_old(Password),
    {S1, S2} = do_hash_old(AuthPluginData),
    Seed1 = P1 bxor S1,
    Seed2 = P2 bxor S2,
    {Extra, List} = rnd(9, Seed1, Seed2),
	list_to_binary([E bxor Extra || E <- List]);
hash_old(AuthPluginData, Password) when is_list(Password) ->
	hash_old(binary_to_list(AuthPluginData), Password);
hash_old(AuthPluginData, Password) when is_binary(Password) ->
	hash_old(binary_to_list(AuthPluginData), binary_to_list(Password)).
	
do_hash_old(S) ->
    do_hash_old(S, ?CODE_N1, ?CODE_N2, ?CODE_SUM).

do_hash_old([C | S], N1, N2, Add) ->
    N1_1 = N1 bxor (((N1 band 63) + Add) * C + N1 * 256),
    N2_1 = N2 + ((N2 * 256) bxor N1_1),
    Add_1 = Add + C,
    do_hash_old(S, N1_1, N2_1, Add_1);
do_hash_old([], N1, N2, _Add) ->
    {N1 band ?MASK_31 , N2 band ?MASK_31}.

rnd(N, Seed1, Seed2) ->
    rnd(N, [], Seed1 rem ?MASK_30, Seed2 rem ?MASK_30).

rnd(0, [H|T], _, _) ->
    {H - 64, lists:reverse(T)};
rnd(N, List, Seed1, Seed2) ->
    NSeed1 = (Seed1 * 3 + Seed2) rem ?MASK_30,
    NSeed2 = (NSeed1 + Seed2 + 33) rem ?MASK_30,
    Float = (float(NSeed1) / float(?MASK_30)) * 31,
    Val = trunc(Float) + 64,
    rnd(N - 1, [Val | List], NSeed1, NSeed2).

bxor_binary(B1, B2) ->
	Fun =
	fun(E1, {[E2|T], Acc}) ->
		{T, [E1 bxor E2|Acc]}
	end,
	{_, List} = lists:foldl(Fun, {binary_to_list(B2), []}, binary_to_list(B1)),
	list_to_binary(lists:reverse(List)).

%% for handshake
parser_handshake(Packet, Capability) ->
	case parser_gen_packet(Packet, Capability) of
		{error, unknown_packet_form} ->
			auth_switch(Packet);
		Result ->
			Result
	end.

%% for query
parser_query(Packet, Capability, Step) ->
	case parser_gen_packet(Packet, Capability) of
		{error, unknown_packet_form} ->
			query_packet(Packet, Capability, Step);
		Result ->
			Result
	end.

%% for general packet
parser_gen_packet(<<0:8, Packet/binary>>, Capability) ->
	ok_packet(Packet, Capability);
parser_gen_packet(<<254:8, Packet/binary>>, Capability) ->
	Protocal41Flags = ?CLIENT_PROTOCOL_41 band Capability,
	eof_packet(Packet, Protocal41Flags);
parser_gen_packet(<<255:8, Packet/binary>>, Capability) ->
	Protocal41Flags = ?CLIENT_PROTOCOL_41 band Capability,
	error_packet(Packet, Protocal41Flags);
parser_gen_packet(_Packet, _Capability) ->
	{error, unknown_packet_form}.
	
%% OK_Packet
ok_packet(Bin, Capability) ->
	{AffectedRows, Rest1} = parser_integer(Bin),
	{LastInsertId, Rest2} = parser_integer(Rest1),
	if
		?CLIENT_PROTOCOL_41 == ?CLIENT_PROTOCOL_41 band Capability ->
			<<StatusFlags:16/little, Warnings:16/little, Rest3/binary>> = Rest2;
		?CLIENT_TRANSACTIONS == ?CLIENT_TRANSACTIONS band Capability ->
			<<StatusFlags:16/little, Rest3/binary>> = Rest2,
			Warnings = 0;
		true ->
			Rest3 = Rest2,
			StatusFlags = 0,
			Warnings = 0
	end,
	case ?CLIENT_SESSION_TRACK == ?CLIENT_SESSION_TRACK band Capability of
		true ->
			{Info, Rest4} = parser_string_lenenc(Rest3),
			case ?SERVER_SESSION_STATE_CHANGED == ?SERVER_SESSION_STATE_CHANGED band StatusFlags of
				true ->
					{SessionStateChangesBin, _Rest5} = parser_string_lenenc(Rest4),
					SessionStateChanges = parser_session_state_changes(SessionStateChangesBin, []);
				false ->
					SessionStateChanges = []
			end;
		false ->
			Info = Rest3,
			SessionStateChanges = []
	end,
	{ok, [AffectedRows, LastInsertId, Warnings, Info, SessionStateChanges]}.

parser_session_state_changes(<<Type:8, Rest/binary>>, Result) ->
	{B, NewRest} = parser_string_lenenc(Rest),
	S = do_parser_session_state_change(Type, B, []),
	parser_session_state_changes(NewRest, [{Type, S}|Result]);
parser_session_state_changes(<<>>, Result) ->
	lists:reverse(Result).

do_parser_session_state_change(_Type, <<>>, Result) ->
	lists:reverse(Result);
do_parser_session_state_change(?SESSION_TRACK_SYSTEM_VARIABLES, Bin, Result) ->
	{Name, Bin1} = parser_string_lenenc(Bin),
	{Value, Bin2} = parser_string_lenenc(Bin1),
	do_parser_session_state_change(?SESSION_TRACK_SYSTEM_VARIABLES, Bin2, [{Name, Value}|Result]);
do_parser_session_state_change(?SESSION_TRACK_SCHEMA, Bin, Result) ->
	{Name, Bin1} = parser_string_lenenc(Bin),
	do_parser_session_state_change(?SESSION_TRACK_SCHEMA, Bin1, [Name|Result]);
do_parser_session_state_change(?SESSION_TRACK_STATE_CHANGE, Bin, Result) ->
	{IsTracked, Bin1} = parser_string_lenenc(Bin),
	do_parser_session_state_change(?SESSION_TRACK_STATE_CHANGE, Bin1, [IsTracked|Result]).

%% EOF Packet
eof_packet(<<Warnings:16/little, StatusFlags:16/little>>, ?CLIENT_PROTOCOL_41) ->
	{eof, [Warnings, StatusFlags]};
eof_packet(<<>>, 0) ->
	{eof, [0, 0]}.

%% ERR_Packet
error_packet(<<Code:16/little, _SqlStateMarker:8, SqlState:5/binary, Message/binary>>, ?CLIENT_PROTOCOL_41) ->
	{error, [Code, {binary_to_list(SqlState), binary_to_list(Message)}]};
error_packet(<<Code:16/little, Message/binary>>, 0) ->
	{error, [Code, binary_to_list(Message)]}.
	
query_request(String, Index) when is_list(String) ->
	Bin = iolist_to_binary(String),
	query_request(Bin, Index);
query_request(Bin, Index) when is_binary(Bin) ->
	Len = byte_size(Bin) + 1,
	<<Len:24/little, Index:8, ?COM_QUERY:8, Bin/binary>>.
	
%% for handshake result, now is ignored
auth_switch(<<254:8, Rest/binary>>) ->
	case Rest == <<>> of
		true ->
			{auth_switch, [<<>>, <<>>]};
		false ->
			{PluginName, Rest1} = parser_string_null(Rest),
			{AuthPluginData, _Rest2} = parser_string_null(Rest1),
			{auth_switch, [PluginName, AuthPluginData]}
	end;
auth_switch(_Bin) ->
	{error, unknown_packet_form}.

%% for query result
query_packet(Packet, _Capability, 0) ->
	case parser_integer(Packet) of
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
	RowInfo = parser_string_lenencs(Packet, []),
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
	{Catalog, Rest1} = parser_string_lenenc(Bin),
	{Schema, Rest2} = parser_string_lenenc(Rest1),
	{Table, Rest3} = parser_string_lenenc(Rest2),
	{OrgTable, Rest4} = parser_string_lenenc(Rest3),
	{Name, Rest5} = parser_string_lenenc(Rest4),
	{OrgName, Rest6} = parser_string_lenenc(Rest5),
	{_NextLength, 
	<<
		CharacterSet:16, 
		ColumnLength:32, 
		Type:8, 
		Flags:16, 
		Decimals:8, 
		_Filler:16, 
		Rest7/binary
	>>} = parser_integer(Rest6),
	case COM == ?COM_FIELD_LIST of
		true ->
			{DefaultValues, _Rest8} = parser_string_lenenc(Rest7);
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
	{Table, Rest1} = parser_string_lenenc(Bin),
	{Name, Rest2} = parser_string_lenenc(Rest1),
	{_NextLength, <<ColumnLength:24, Rest3/binary>>} = parser_integer(Rest2),
	{_LengthOfTypeField, <<Type:8, Rest4/binary>>} = parser_integer(Rest3),
	case ?CLIENT_LONG_FLAG == ?CLIENT_LONG_FLAG band Capability of
		true ->
			{_Len1, <<Flags:16, Decimals:8, Rest5/binary>>} = parser_integer(Rest4);
		false ->
			<<_Len1:8, Flags:8, Decimals:8, Rest5/binary>> = Rest4
	end,
	case COM == ?COM_FIELD_LIST of
		true ->
			{DefaultValues, _Rest6} = parser_string_lenenc(Rest5);
		false ->
			DefaultValues = <<>>
	end,
	[Table, Name, ColumnLength, Type, Flags, Decimals, DefaultValues].

%% parser the execute result
parser_result(_Result) ->
	ok.
	