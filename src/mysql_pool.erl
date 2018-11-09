%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_pool).

-include("mysql.hrl").

-export([start/2, stop/1]).

%% @doc 启动池
start(Pool, Opts) ->
    {ok, Pid} = mysql_driver_sup:start_child(Pool, [Opts]),
    case mysql_manager:reg_pool(Pool, Opts) of
        true ->
            Num = proplists:get_value(num, Opts, 1),
            InitFun = case proplists:get_value(db, Opts) of
                undefined ->
                    fun(P, C) -> P ! {ok, C} end;
                DBName ->
                    fun(P, C) ->
                        Res = mysql_lib:execute(Pool, list_to_binary(["use ", DBName])),
                        P ! {Res, C}
                    end
            end,
            start_child(Pid, Num, Pool, self(), InitFun),
            case wait_loop(Pid, Num, 0) of
                0 ->
                    stop(Pool),
                    mysql_manager:unreg_pool(Pool),
                    false;
                Ok ->
                    {true, Ok}
            end;
        false ->
            stop(Pool),
            false
    end.

start_child(SupRef, Num, Pool, Parent, InitFun) when Num > 0 ->
    spawn_link(fun() ->
        {ok, Child} = supervisor:start_child(SupRef, [self(), Pool]),
        receive
            ok -> InitFun(Parent, Child)
        end
    end),
    start_child(SupRef, Num - 1, Pool, Parent, InitFun);
start_child(_SupRef, _Num, _Pool, _Parent, _InitFun) ->
    ok.

wait_loop(SupRef, N, Res) when N > 0 ->
    NewRes = receive
        {ok, _} ->
            Res + 1;
        {Error, Pid} ->
            lager:error("fail connect:~p", [Error]),
            supervisor:terminate_child(SupRef, Pid),
            Res
    end,
    wait_loop(SupRef, N - 1, NewRes);
wait_loop(_SupRef, _N, Res) -> Res.

%% @doc 关闭池
stop(Pool) ->
    mysql_driver_sup:stop_child(Pool).
