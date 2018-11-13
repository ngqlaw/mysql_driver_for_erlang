%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_pool_manager).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([
    start_link/0,
    reg/2,
    get_server/1,
    get_all/0
]).

%% gen_server.
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    pools = #{},    % #{pool_id => pid}
    map = #{}       % #{ref => pool_id}
}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 注册池
reg(Pool, ManagerPid) ->
    gen_server:call(?MODULE, {reg, Pool, ManagerPid}).

%% @doc 取池服务进程
get_server(Pool) ->
    gen_server:call(?MODULE, {get_server, Pool}).

%% @doc 获取所有池服务进程
get_all() ->
    gen_server:call(?MODULE, get_all).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) -> 
    {ok, #state{
        pools = #{},
        map = #{}
    }}.

handle_call({reg, Pool, ManagerPid}, _From, #state{pools = Pools, map = Map} = State) ->
    case maps:get(Pool, Pools, undefined) of
        undefined ->
            NewPools = maps:put(Pool, ManagerPid, Pools),
            % 监控子进程
            NewMap = maps:merge(Map, #{erlang:monitor(process, ManagerPid) => Pool}),
            {reply, true, State#state{pools = NewPools, map = NewMap}};
        _ ->
            {reply, false, State}
    end;
handle_call({get_server, Pool}, _From, #state{pools = Pools} = State) ->
    case maps:get(Pool, Pools, undefined) of
        undefined ->
            {reply, undefined, State};
        ManagerPid ->
            {reply, ManagerPid, State}
    end;
handle_call(get_all, _From, #state{pools = Pools} = State) ->
    ManagerPids = maps:values(Pools),
    {reply, ManagerPids, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, Object, _Info}, #state{pools = Pools, map = Map} = State) ->
    case maps:take(MonitorRef, Map) of
        {Pool, NewMap} ->
            {Object, NewPools} = maps:take(Pool, Pools),
            {noreply, State#state{pools = NewPools, map = NewMap}};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
