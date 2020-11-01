%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_lib).

-include("mysql.hrl").

-export([pool/0, execute/1, stop/0]).

-define(APP, mysql_driver_app).
-define(POOL, mysql_pool).

%%====================================================================
%% API
%%====================================================================
%% @doc mysql池定义
pool() ->
    Size = application:get_env(?APP, pool_size, 5),
    Max = application:get_env(?APP, pool_max_overflow, 10),
    PoolArgs = [
        {name, {local, ?POOL}},
        {worker_module, mysql_handler},
        {size, Size},
        {max_overflow, Max},
        {strategy, lifo}
    ],
    WorkerArgs = [
        {host, application:get_env(?APP, host, "127.0.0.1")},
        {port, application:get_env(?APP, port, 3306)},
        {database, application:get_env(?APP, database, "db")},
        {user, application:get_env(?APP, user, "admin")},
        {password, application:get_env(?APP, password, "******")}
    ],
    poolboy:child_spec(?POOL, PoolArgs, WorkerArgs).

%% @doc execute sql sentence
execute(String) ->
    Pid = poolboy:checkout(?POOL, false),
    execute(Pid, String).
execute(Pid, String) when is_pid(Pid) ->
    mysql_handler:call(Pid, query_sql, String).

%% @doc 关闭mysql池
stop() ->
    poolboy:stop(?POOL).
