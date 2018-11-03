%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_connect).

-include("mysql.hrl").

-export([
	start/1,
	handle_reply/2
]).

%% @doc start connect
start(Opts) ->
	Host = proplists:get_value(host, Opts),
	Port = proplists:get_value(port, Opts),
	gen_tcp:connect(Host, Port, ?OPTIONS).

%% @doc server reply
handle_reply(Data, Opts) ->
	User = proplists:get_value(user, Opts),
	Password = proplists:get_value(password, Opts),
	Handshake = mysql_handshake:decode(Data),
	mysql_auth:handshake_response(Handshake, User, Password).
