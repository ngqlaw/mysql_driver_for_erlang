%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_connect).

-include("mysql.hrl").

-export([
	connect/1,
	handle_connect/2,
	handle_handshake/3
]).

%% @doc 连接数据库
connect(Opts) ->
	Host = proplists:get_value(host, Opts),
	Port = proplists:get_value(port, Opts),
	gen_tcp:connect(Host, Port, ?OPTIONS).

%% @doc 连接结果
handle_connect(Data, Opts) ->
	User = proplists:get_value(db_user, Opts),
	Password = proplists:get_value(db_password, Opts),
	[ConnectID|ServerData] = parser_handshake(Data),
	{ok, ResponseBin, Version, Capability} = response_handshake(ServerData, User, Password),
	{ok, ConnectID, ResponseBin, Version, Capability}.

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

response_handshake([10, Capability, AuthPluginData, CharacterSet, Index], UserName, Password) ->
	case ?CLIENT_PLUGIN_AUTH == Capability band ?CLIENT_PLUGIN_AUTH orelse
		(?CLIENT_PROTOCOL_41 == Capability band ?CLIENT_PROTOCOL_41 andalso
		?CLIENT_SECURE_CONNECTION == Capability band ?CLIENT_SECURE_CONNECTION) of
		true ->
			ResponseBin = handshake_response_41(UserName, Password, CharacterSet, AuthPluginData, Index + 1);
		false ->
			ResponseBin = handshake_response_320(UserName, Password, AuthPluginData, Index + 1)
	end,
	{ok, ResponseBin, 10, Capability};
response_handshake([9, AuthPluginData, Index], UserName, Password) ->
	ResponseBin = handshake_response_320(UserName, Password, AuthPluginData, Index + 1),
	{ok, ResponseBin, 9, ?CLIENT_TRANSACTIONS}.

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

%% @doc 握手结果
handle_handshake(<<_:24/little, _Index:8, Rest/binary>>, Capability, Opts) ->
	case mysql_util:parser_handshake(Rest, Capability) of
		{ok, _OK} ->
			DB = proplists:get_value(db_name, Opts),
			DBBin = iolist_to_binary(DB),
			{ok, <<"USE `", DBBin/binary, "`">>};
		{eof, EOF} -> %% for now is not suppose to be match
			{error, {unexpected, EOF}};
		{error, Error} ->
			{error, Error}
	end.

