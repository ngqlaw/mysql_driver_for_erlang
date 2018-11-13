%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_pool_sup).

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

%% API
-export([start_link/2]).

-define(CHILD(Mod, Args), {Mod, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}).
-define(CHILD(Id, Mod, Args), {Id, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}).

start_link(Id, Opts) ->
    supervisor:start_link(?MODULE, [Id, Opts]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([Id, Opts]) ->
    Manager = ?CHILD(mysql_manager, [Id]),
    SupSpec = ?CHILD(Id, mysql_sup, [Id, Opts]),
    {ok, { {one_for_all, 3, 10}, [Manager, SupSpec]} }.