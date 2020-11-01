%%%-------------------------------------------------------------------
%% @doc mysql_driver top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mysql_driver_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Manager = mysql_lib:pool(),
    {ok, { {one_for_one, 3, 10}, [Manager]} }.

%%====================================================================
%% Internal functions
%%====================================================================
