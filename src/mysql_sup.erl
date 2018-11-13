%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_sup).

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

%% API
-export([start_link/2]).

start_link(Pool, Opts) ->
	supervisor:start_link(?MODULE, [Pool, Opts]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([Pool, Opts]) ->
	Num = proplists:get_value(num, Opts, 1),
	Db = proplists:get_value(db, Opts),
	SupPid = self(),
    Children = [begin
    	Id = {mysql_handler, N},
    	InitFun = case Db of
	        undefined ->
	            fun(_Pid) -> ok end;
	        DBName ->
	            fun(Pid) ->
	            	case mysql_lib:execute(Pid, list_to_binary(["use ", DBName])) of
	            		ok ->
	            			ok;
	            		Error ->
	            			lager:error("fail connect:~p", [Error]),
				            supervisor:terminate_child(SupPid, Id),
				            supervisor:delete_child(SupPid, Id),
				            skip
	            	end
	            end
	    end,
	    {
			Id,
			{mysql_handler, start_link, [Pool, InitFun, Opts]},
			permanent,
			5000,
			worker,
			[mysql_handler]
		} end || N <- lists:seq(1, Num)],
    {ok, { {one_for_one, Num * 3, 10}, Children} }.
