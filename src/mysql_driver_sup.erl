%%%-------------------------------------------------------------------
%% @doc mysql_driver top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mysql_driver_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/2, restart_child/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Id, Opts) ->
    ChildSpec = {Id, {mysql_sup, start_link, [Opts]}, permanent, 5000, worker, [mysql_sup]},
    supervisor:start_child(?SERVER, ChildSpec).

restart_child(Id) ->
    ok = supervisor:terminate_child(?SERVER, Id),
    {ok, _} = supervisor:restart_child(?SERVER, Id),
    ok.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Manager = {mysql_manager, {mysql_manager, start_link, []}, permanent, 5000, worker, [mysql_manager]},
    {ok, { {one_for_all, 3, 10}, [Manager]} }.

%%====================================================================
%% Internal functions
%%====================================================================
