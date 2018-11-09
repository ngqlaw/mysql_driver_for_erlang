%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_pool).

-include("mysql.hrl").

-export([start/2, stop/1]).

%% @doc 启动池
start(Pool, Opts) ->
    {ok, Pid} = mysql_driver_sup:start_child(Pool, [Opts]),
    Num = proplists:get_value(num, Opts, 1),
    InitFun = case proplists:get_value(db, Opts) of
        undefined ->
            fun(P) -> P ! ok end;
        DBName ->
            fun(P) ->
                Res = mysql_lib:execute(Pool, list_to_binary(["use ", DBName])),
                P ! Res
            end
    end,
    start_child(Pid, Num, Pool, self(), InitFun),
    wait_loop(Num).

start_child(SupRef, Num, Pool, Parent, InitFun) when Num > 0 ->
    spawn_link(fun() ->
        {ok, _Child} = supervisor:start_child(SupRef, [self(), Pool]),
        receive
            ok -> InitFun(Parent)
        end
    end),
    start_child(SupRef, Num - 1, Pool, Parent, InitFun);
start_child(_SupRef, _Num, _Pool, _Parent, _InitFun) ->
    ok.

wait_loop(N) when N > 0 ->
    receive
        ok -> ok;
        Error ->
            lager:error("fail connect:~p", [Error])
    end,
    wait_loop(N - 1);
wait_loop(_) -> ok.

%% @doc 关闭池
stop(Pool) ->
    mysql_driver_sup:stop_child(Pool).
