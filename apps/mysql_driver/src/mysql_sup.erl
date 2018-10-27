%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_sup).

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

%% API
-export([start_link/0, start_child/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).
	
start_child(Args) ->
	supervisor:start_child(?MODULE, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([]) ->
	ets:new(mysql_conn, [public, set, named_table, {keypos, 1}]),
	ChildSpec = {
		mysql_handler,
		{mysql_handler, start_link, []},
		temporary,
		5000,
		worker,
		[mysql_handler]
	},
    {ok, { {simple_one_for_one, 3, 60}, [ChildSpec]} }.