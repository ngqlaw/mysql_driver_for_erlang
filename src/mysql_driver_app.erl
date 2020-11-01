%%%-------------------------------------------------------------------
%% @doc mysql_driver public API
%% @author ningguoqiang <ngq_scut@126.com>
%% 
%% @end
%%%-------------------------------------------------------------------

-module(mysql_driver_app).

-behaviour(application).

-export([start/0]).
%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================
%% @doc 手动调用启动
start() ->
    start(mysql_driver).

%% @doc app启动
start(_StartType, _StartArgs) ->
    mysql_driver_sup:start_link().

%% @doc app关闭
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
%% 启动app
start(App) ->
    start_ok(App, application:start(App, permanent)).

start_ok(_App, ok) -> ok;
start_ok(_App, {error, {already_started, _App}}) -> ok;
start_ok(App, {error, {not_started, Dep}}) ->
    ok = start(Dep),
    start(App);
start_ok(App, {error, Reason}) ->
    erlang:error({app_start_failed, App, Reason}).
