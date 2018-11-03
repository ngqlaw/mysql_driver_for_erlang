%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_sup).

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

%% API
-export([start_link/1]).

start_link(Args) ->
	supervisor:start_link(?MODULE, Args).

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