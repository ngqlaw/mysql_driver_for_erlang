%%%-------------------------------------------------------------------
%% @doc mysql_driver public API
%% @end
%%%-------------------------------------------------------------------

-module(mysql_driver_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    mysql_driver_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================