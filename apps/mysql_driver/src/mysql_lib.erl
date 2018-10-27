%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_lib).

-include("mysql.hrl").

-export([start/0]).
-export([execute/1, configure/1]).

-export([reader/2]).

start() ->
	ok = application:ensure_started(mysql),
	{ok, Host} = application:get_env(mysql, host),
	{ok, Port} = application:get_env(mysql, port),
	{ok, DB} = application:get_env(mysql, db_name),
	{ok, User} = application:get_env(mysql, db_user),
	{ok, Password} = application:get_env(mysql, db_password),
	{ok, Num} = application:get_env(mysql, child_num),
	Fun =
	fun(Pid) ->
		DBBin = iolist_to_binary(DB),
		Bin = <<"USE `", DBBin/binary, "`">>,
		mysql_handler:send(Pid, query, Bin)
	end,
	init(Num, [Host, Port, Fun, User, Password]),
	Acc = wait_init(Num, []),
	Acc.
	
init(Num, Args) when Num > 0 ->
	erlang:spawn(?MODULE, reader, [self(), Args]),
	init(Num - 1, Args);
init(_Num, _Args) ->
	ok.
	
wait_init(Num, Acc) when Num > 0 ->
	receive
		{ok, OK} ->
			NewAcc = [OK|Acc];
		Error ->
			NewAcc = [{error, Error}|Acc]
	end,
	wait_init(Num - 1, NewAcc);
wait_init(_Num, Acc) ->
	Acc.

reader(ParentPid, Args) ->
	Result = mysql_connect:start(Args),
	ParentPid ! Result.

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