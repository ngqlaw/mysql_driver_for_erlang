%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_lib).

-include("mysql.hrl").

-export([
    pool/0,
    execute/1, execute/3,
    get_handle/0,
    release_handle/1
]).

-define(APP, mysql_driver).
-define(POOL, mysql_pool).

%%====================================================================
%% API
%%====================================================================
%% @doc mysql池定义
pool() ->
    {ok, Size} = application:get_env(?APP, pool_size),
    {ok, Max} = application:get_env(?APP, pool_max_overflow),
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
        {password, application:get_env(?APP, password, "********")}
    ],
    poolboy:child_spec(?POOL, PoolArgs, WorkerArgs).

%% @doc execute sql sentence
execute(String) ->
    Pid = poolboy:checkout(?POOL, true),
    execute(Pid, String, true).
execute(Pid, String, IsRelease) when is_pid(Pid) ->
    Res = mysql_handler:call(Pid, query_sql, String),
    case IsRelease of
        true ->
            poolboy:checkin(?POOL, Pid);
        false ->
            skip
    end,
    Res.

%% @doc 取得mysql处理进程
get_handle() ->
    poolboy:checkout(?POOL, false).

%% @doc 释放mysql处理进程
release_handle(Pid) ->
    poolboy:checkin(?POOL, Pid).

%%%===================================================================
%%% Internal functions
%%%===================================================================
