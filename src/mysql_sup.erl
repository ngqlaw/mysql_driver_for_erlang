%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_sup).

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

%% API
-export([start_link/1, start_child/3]).

start_link(Args) ->
	supervisor:start_link(?MODULE, Args).
	
start_child(SupRef, Num, Args) when Num > 0 ->
	{ok, _Child} = supervisor:start_child(SupRef, Args),
	start_child(SupRef, Num - 1, Args);
start_child(_SupRef, _Num, _Args) ->
	ok.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init(Args) ->
	ChildSpec = {
		mysql_handler,
		{mysql_handler, start_link, Args},
		temporary,
		5000,
		worker,
		[mysql_handler]
	},
    {ok, { {simple_one_for_one, 3, 10}, [ChildSpec]} }.