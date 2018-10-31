%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_app).

-export([start/0, start/2]).

start() ->
	start(mysql),
	mysql_lib:start(),
	ok.
	
start(App) ->
    start_ok(App, application:start(App, permanent)).

start_ok(_App, ok) -> ok;
start_ok(_App, {error, {already_started, _App}}) -> ok;
start_ok(App, {error, {not_started, Dep}}) ->
    ok = start(Dep),
    start(App);
start_ok(App, {error, Reason}) ->
    erlang:error({app_start_failed, App, Reason}).	
	
start(normal, _) ->
	mysql_sup:start_link().