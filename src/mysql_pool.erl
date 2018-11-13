%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_pool).

-include("mysql.hrl").

-export([start/2, stop/1]).

%% @doc 启动池
start(Pool, Opts) ->
    mysql_driver_sup:start_child(Pool, Opts).

%% @doc 关闭池
stop(Pool) ->
    mysql_driver_sup:stop_child(Pool).
