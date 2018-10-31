%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_lib).

-include("mysql.hrl").

-export([execute/1, configure/1]).

%% @doc execute sql sentence
execute(String) ->
	case ets:tab2list(mysql_conn) of
		[] ->
			{error, no_mysql_handler};
		List ->
			Len = length(List),
			N = random:uniform(Len),
			{_, Pid} = lists:nth(N, List),
			mysql_handler:send(Pid, query, String)
	end.

%% @doc configure all connects
configure(String) ->
	case ets:tab2list(mysql_conn) of
		[] ->
			{error, no_mysql_handler};
		List ->
			Result = [mysql_handler:send(Pid, query, String) || {_, Pid} <- List],
			{ok, Result}
	end.
	
-ifdef(TEST).
-export([test_select/0, test_update/0, test_replace/0, test_alter/0]).

test_select() ->
	String = <<"select * from `test`">>,
	execute(String).	

test_update() ->
	ok.
	
test_replace() ->
	ok.
	
test_alter() ->
	ok.
	
-endif.