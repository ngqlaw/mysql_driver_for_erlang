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
	DB = proplists:get_value(db_name, Opts),
	Handshake = mysql_packet:handshake(Data),
	mysql_auth:handshake_response(Handshake, User, Password, DB).

%% @doc 握手结果
handle_handshake(Data, Capability, _Opts) ->
	#mysql_packet{payload = Payload} = mysql_packet:packet(Data, <<>>, Capability),
	lager:info("handshake result:~p", [Payload]),
	case Payload of
		#mysql_ok_packet{} ->
			ok;
		#mysql_err_packet{code = Code, message = Msg} ->
			{error, {Code, Msg}};
		Error ->
			Error
	end.

