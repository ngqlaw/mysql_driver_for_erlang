%%%-------------------------------------------------------------------
%% @doc mysql_driver top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mysql_driver_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_child/2,
    get_child/1,
    restart_child/1,
    stop_child/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-define(CHILD(Mod, Args), {Mod, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}).
-define(CHILD(Id, Mod, Args), {Id, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Id, Opts) ->
    SupSpec = ?CHILD(Id, mysql_pool_sup, [Id, Opts]),
    supervisor:start_child(?SERVER, SupSpec).

get_child(Id) ->
    L = supervisor:which_children(?SERVER),
    case lists:keyfind(Id, 1, L) of
        {_, Pid, _, _} -> Pid;
        _ -> undefined
    end.

restart_child(Id) ->
    case supervisor:terminate_child(?SERVER, Id) of
        ok ->
            supervisor:restart_child(?SERVER, Id);
        Error ->
            Error
    end.

stop_child(Id) ->
    case supervisor:terminate_child(?SERVER, Id) of
        ok ->
            supervisor:delete_child(?SERVER, Id);
        Error ->
            Error
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Manager = ?CHILD(mysql_pool_manager, []),
    {ok, { {one_for_all, 3, 10}, [Manager]} }.

%%====================================================================
%% Internal functions
%%====================================================================
