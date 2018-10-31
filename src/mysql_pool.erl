%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_pool).

-include("mysql.hrl").

-export([start/2]).

start(Pool, Opts) ->
    {ok, Pid} = mysql_driver_sup:start_child(Pool, [Opts]),
    Num = proplists:get_value(num, Opts, 3),
    mysql_sup:start_child(Pid, Num, [Pool]).
