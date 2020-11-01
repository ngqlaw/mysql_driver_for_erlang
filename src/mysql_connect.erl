%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_connect).

-include("mysql.hrl").

-export([
	start/1
]).

init_handle(Flag) ->
    #mysql_handle{
        flag = Flag,
        packet = #mysql_packet{},
        result = #mysql_result{}
    }.

%% @doc start connect
start(Opts) ->
	Host = proplists:get_value(host, Opts),
	Port = proplists:get_value(port, Opts),
    User = proplists:get_value(user, Opts),
    Password = proplists:get_value(password, Opts),
	case gen_tcp:connect(Host, Port, ?OPTIONS) of
        {ok, Socket} ->
            case start_handshake(Socket, User, Password) of
                {ok, ConnectId, Capability} ->
                    case use_database(Socket, Capability, Opts) of
                        ok ->
                            {ok, ConnectId, Socket, Capability};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

start_handshake(Socket, User, Password) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            {Handshake, _} = mysql_packet:decode(Data, <<>>, 0, ?MYSQL_FLAG_HANDSHAKE),
            #mysql_handshake_response{
                connect_id = ConnectId,
                version = Version,
                capability = Capability,
                response = ResponseBin
            } = mysql_auth:handshake_response(Handshake, User, Password),
            gen_tcp:send(Socket, ResponseBin),
            Handle = init_handle(?MYSQL_FLAG_HANDSHAKE),
            case handshake(Socket, Version, Capability, Handle) of
                ok ->
                    {ok, ConnectId, Capability};
                Error ->
                    Error
            end;
        Other ->
            {start_fail, Other}
    end.

handshake(Socket, Version, Capability, Handle) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            case mysql_route:routing(Handle, Capability, Data) of
                {need_more, NewHandle} ->
                    %% TODO
                    %% Client and server possibly exchange further packets 
                    %% as required by the server authentication method for 
                    %% the user account the client is trying to authenticate against.
                    %% !!! not suport now !!!
                    handshake(Socket, Version, Capability, NewHandle);
                {continue, NewHandle} ->
                    handshake(Socket, Version, Capability, NewHandle);
                {ok, _NewHandle} ->
                    ok;
                {error, Error} ->
                    Error
            end;
        Other ->
            {handshake_fail, Other}
    after 3000 ->
        handshake_timeout
    end.

use_database(Socket, Capability, Opts) ->
    case proplists:get_value(database, Opts) of
        DBName when is_list(DBName) orelse is_binary(DBName) ->
            SendBin = mysql_command:encode(?COM_QUERY, list_to_binary(["use ", DBName, ";"]), 0),
            gen_tcp:send(Socket, SendBin),
            Handle = init_handle(?MYSQL_FLAG_QUERY),
            use_database_loop(Socket, Capability, Handle);
        DB ->
            {bad_db_name, DB}
    end.

use_database_loop(Socket, Capability, Handle) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            case mysql_route:routing(Handle, Capability, Data) of
                {ok, _NewHandle} ->
                    ok;
                {need_more, NewHandle} ->
                    use_database_loop(Socket, Capability, NewHandle);
                {continue, NewHandle} ->
                    use_database_loop(Socket, Capability, NewHandle);
                {error, Error} ->
                    Error
            end;
        Error ->
            {use_database_fail, Error}
    end.
