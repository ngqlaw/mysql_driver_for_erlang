%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_connect).

-include("mysql.hrl").

-export([start/1]).

start([Host, Port, InitFun, User, Password]) ->
	case gen_tcp:connect(Host, Port, ?OPTIONS) of
		{ok, Socket} ->
			receive
				{tcp, Socket, Data} ->
					[ConnectID|ServerData] = parser_handshake(Data),
					case response_handshake(ServerData, User, Password, Socket) of
						{ok, ClientData} ->
							start_handler(Socket, [InitFun, ConnectID|ClientData]);
						Result ->
							Result
					end;
				{tcp_closed, Socket} ->
					gen_tcp:close(Socket),
					{error, tcp_closed};
				{tcp_error, Socket, Error} ->
					gen_tcp:close(Socket),
					{error, {tcp_error, Error}}
			end;
		{error, Reason} ->
			{error, {connect_fail, Reason}}
	end.
	
start_handler(Socket, [InitFun, ConnectID|Args]) ->
	case mysql_sup:start_child([[Socket, ConnectID|Args]]) of
		{ok, Pid} ->
			case gen_tcp:controlling_process(Socket, Pid) of
				ok ->
					case InitFun(Pid) of
						ok ->
							ets:insert(mysql_conn, {ConnectID, Pid}),
							{ok, [Pid|Args]};
						Error ->
							{error, {init_connect_fail, Error}}
					end;
				Error ->
					{error, {handle_socket_error, Error}}
			end;
		Error ->
			{error, {start_handler_error, Error}}
	end.

parser_handshake(<<_ByteLen:24/little, Index:8, 10:8, Rest/binary>>) ->
	{_Version, <<ConnectID:32/little, Rest1/binary>>} = mysql_util:parser_string_null(Rest),
	{AuthPluginDataPart1, <<0:8, LowerCapabilityFlags:16/little, Rest2/binary>>} = erlang:split_binary(Rest1, 8),
	case Rest2 of
		<<CharacterSet:8, _StatusFlags:16, UpperCapabilityFlags:16/little, Len:8, Rest3/binary>> ->
			{_, Rest4} = erlang:split_binary(Rest3, 10),
			Capability = (UpperCapabilityFlags bsl 16) + LowerCapabilityFlags,
			case Capability band ?CLIENT_SECURE_CONNECTION of
				?CLIENT_SECURE_CONNECTION ->
					Bytes = max(13, Len - 8) - 1,
					{AuthPluginDataPart2, <<0:8, _Rest5/binary>>} = erlang:split_binary(Rest4, Bytes),
					AuthPluginData = <<AuthPluginDataPart1/binary, AuthPluginDataPart2/binary>>;
				_ ->
					%Rest5 = Rest4,
					AuthPluginData = AuthPluginDataPart1
			end,
			%case Capability band ?CLIENT_PLUGIN_AUTH of
			%	?CLIENT_PLUGIN_AUTH ->
			%		{AuthPluginName, _} = parser_string_null(Rest5);
			%	_ ->
			%		AuthPluginName = <<>>
			%end,
			[ConnectID, 10, Capability, AuthPluginData, CharacterSet, Index];
		_ ->
			[ConnectID, 10, LowerCapabilityFlags, AuthPluginDataPart1, 8, Index]
	end;
parser_handshake(<<_:24/little, Index:8, 9:8, Rest/binary>>) ->	
	{_Version, <<ConnectID:32/little, Rest1/binary>>} = mysql_util:parser_string_null(Rest),
	{AuthPluginData, _} = mysql_util:parser_string_null(Rest1),
	[ConnectID, 9, AuthPluginData, Index].

response_handshake([10, Capability, AuthPluginData, CharacterSet, Index], UserName, Password, Socket) ->
	case ?CLIENT_PLUGIN_AUTH == Capability band ?CLIENT_PLUGIN_AUTH orelse
		(?CLIENT_PROTOCOL_41 == Capability band ?CLIENT_PROTOCOL_41 andalso
		?CLIENT_SECURE_CONNECTION == Capability band ?CLIENT_SECURE_CONNECTION) of
		true ->
			ResponseBin = handshake_response_41(UserName, Password, CharacterSet, AuthPluginData, Index + 1);
		false ->
			ResponseBin = handshake_response_320(UserName, Password, AuthPluginData, Index + 1)
	end,
	do_response_handshake(Socket, ResponseBin, 10, Capability);
response_handshake([9, AuthPluginData, Index], UserName, Password, Socket) ->
	ResponseBin = handshake_response_320(UserName, Password, AuthPluginData, Index + 1),
	do_response_handshake(Socket, ResponseBin, 9, ?CLIENT_TRANSACTIONS).
	
do_response_handshake(Socket, ResponseBin, Version, Capability) ->
	gen_tcp:send(Socket, ResponseBin),
	receive
		{tcp, Socket, <<_:24/little, _Index:8, Rest/binary>>} ->
			case mysql_util:parser_handshake(Rest, Capability) of
				{ok, _OK} ->
					{ok, [Version, Capability]};
				{eof, EOF} -> %% for now is not suppose to be match
					{error, {unexpected, EOF}};
				{error, Error} ->
					{error, Error}
			end;
		{tcp_closed, _Socket} ->
			{error, tcp_closed};
		{tcp_error, _Socket, E} ->
			{error, {tcp_error, E}}
	end.
	
handshake_response_41(UserName, Password, CharacterSet, AuthPluginData, Index) ->
	Capability = 
	?CLIENT_PROTOCOL_41 bor 
	?CLIENT_LONG_PASSWORD bor 
	?CLIENT_LONG_FLAG bor
	?CLIENT_TRANSACTIONS bor
	?CLIENT_SECURE_CONNECTION,
	NameBin = list_to_binary(UserName),
	PwdBin = mysql_util:hash(AuthPluginData, Password),
	LPwdBin = byte_size(PwdBin),
	Bin = 
	<<Capability:32/little, 
	?MAX_PACKET_SIZE:32/little, 
	CharacterSet:8,
	0:23/integer-unit:8, 
	NameBin/binary, 0:8, 
	LPwdBin:8, PwdBin/binary>>,
	Len = byte_size(Bin),
	<<Len:24/little, Index:8, Bin/binary>>.
	
handshake_response_320(UserName, Password, AuthPluginData, Index) ->
	{AuthResponse, _} = erlang:split_binary(AuthPluginData, 8),
	Capability = 
	?CLIENT_LONG_PASSWORD bor 
	?CLIENT_LONG_FLAG bor
	?CLIENT_TRANSACTIONS,
	NameBin = list_to_binary(UserName),
	PwdBin = mysql_util:hash_old(AuthResponse, Password),
	Bin =
	<<Capability:16/little, 
	?MAX_PACKET_SIZE:24/little, 
	NameBin/binary, 0:8, 
	PwdBin/binary, 0:8>>,
	Len = byte_size(Bin),
	<<Len:24/little, Index:8, Bin/binary>>.
