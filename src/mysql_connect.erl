%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_connect).

-include("mysql.hrl").

-export([
	start/1
]).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc start connect
start(Opts) ->
	Host = proplists:get_value(host, Opts),
	Port = proplists:get_value(port, Opts),
    User = proplists:get_value(user, Opts),
    Password = proplists:get_value(password, Opts),
    try
        {ok, Socket} = gen_tcp:connect(Host, Port, ?OPTIONS),
        {ok, ConnectId, Capability} = start_handshake(Socket, User, Password),
        ok = use_database(Socket, Capability, Opts),
        {ok, ConnectId, Socket, Capability}
    catch
        ?EXCEPTION(Class, Reason, Stacktrace) -> {Class, Reason, ?GET_STACK(Stacktrace)}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% 发起握手
start_handshake(Socket, User, Password) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            {#mysql_packet{
                payload = #mysql_handshake{version_desc = ServerVersion, scramble = Scramble}
            } = Handshake, _} = mysql_packet:decode(Data, <<>>, 0, ?MYSQL_FLAG_HANDSHAKE),
            #mysql_handshake_response{
                connect_id = ConnectId,
                capability = Capability,
                response = ResponseBin
            } = mysql_auth:handshake_response(Handshake, User, Password),
            gen_tcp:send(Socket, ResponseBin),
            Handle = init_handle(?MYSQL_FLAG_HANDSHAKE),
            case handshake(Socket, ServerVersion, Scramble, Password, Capability, Handle) of
                ok ->
                    {ok, ConnectId, Capability};
                Error ->
                    Error
            end;
        Other ->
            {start_fail, Other}
    after 3000 -> start_handshake_timeout
    end.

%% 握手
handshake(Socket, ServerVersion, Scramble, Password, Capability, Handle) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            case mysql_route:routing(Handle, Capability, Data) of
                {need_more, NewHandle} ->
                    handshake(Socket, ServerVersion, Scramble, Password, Capability, NewHandle);
                {continue, #mysql_handle{
                    packet = #mysql_packet{
                        sequence_id = Index,
                        payload = #mysql_auth_more{data = <<3:8>>}
                    }
                } = NewHandle0} ->
                    %% fast auth,next is ok packet
                    NewHandle = NewHandle0#mysql_handle{
                        packet = #mysql_packet{sequence_id = Index}
                    },
                    handshake(Socket, ServerVersion, Scramble, Password, Capability, NewHandle);
                {continue, #mysql_handle{
                    packet = #mysql_packet{
                        sequence_id = Index,
                        payload = #mysql_auth_more{data = <<4:8>>}
                    }
                } = NewHandle0} ->
                    %% https://github.com/mysql-otp/mysql-otp/blob/master/src/mysql_protocol.erl
                    %% Server wants full authentication (probably specific to the
                    %% caching_sha2_password method), and we are not on a secure channel.
                    %% Since we are not implementing the client-side caching of the server's
                    %% public key, we must ask for it by sending a single byte "2".
                    SendBin = mysql_command:encode(?COM_TEXT, <<2:8>>, Index + 1),
                    gen_tcp:send(Socket, SendBin),
                    NewHandle = NewHandle0#mysql_handle{
                        packet = #mysql_packet{sequence_id = Index}
                    },
                    handshake(Socket, ServerVersion, Scramble, Password, Capability, NewHandle);
                {continue, #mysql_handle{
                    packet = #mysql_packet{
                        sequence_id = Index,
                        payload = #mysql_auth_more{data = PublicKey}
                    }
                } = NewHandle0} ->
                    %% Serveri has sent its public key (certainly specific to the
                    %% caching_sha2_password method). We encrypt the password with
                    %% the public key we received and send it back to the server.
                    Response = mysql_auth:auth_more(ServerVersion, Scramble, Password, PublicKey),
                    SendBin = mysql_command:encode(?COM_TEXT, Response, Index + 1),
                    gen_tcp:send(Socket, SendBin),
                    NewHandle = NewHandle0#mysql_handle{
                        packet = #mysql_packet{sequence_id = Index}
                    },
                    handshake(Socket, ServerVersion, Scramble, Password, Capability, NewHandle);
                {ok, _NewHandle} ->
                    ok;
                {error, Error} ->
                    Error
            end;
        Other ->
            {handshake_fail, Other}
    after 3000 -> handshake_timeout
    end.

%% 切换到设置的数据库
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
    after 3000 -> init_db_timeout
    end.

%% 初始化请求包
init_handle(Flag) ->
    #mysql_handle{
        flag = Flag, packet = #mysql_packet{}, result = #mysql_result{}
    }.
