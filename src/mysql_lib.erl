%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_lib).

-include("mysql.hrl").

-export([execute/2, configure/2]).

%% @doc execute sql sentence
execute(Pid, String) when is_pid(Pid) ->
    mysql_handler:call(Pid, query_sql, String);
execute(undefined, _String) ->
    {error, no_connect_server};
execute(Pool, String) ->
	Pid = mysql_manager:get_server(Pool),
    execute(Pid, String).
	

%% @doc configure all connects
configure(Pool, String) ->
	[mysql_handler:call(Pid, query_sql, String) || Pid <- mysql_manager:get_all(Pool)].
