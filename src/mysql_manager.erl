%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_manager).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([start_link/0, reg/2, get_server/1]).

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
    pools = [],     % [#manager_info{}]
    map = #{}       % %{ref => pool_id}
}).

-record(manager_info, {
    pool_id,
    index = 0,
    pids = []
}).

%% API.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 注册池进程
reg(Pool, Pid) ->
    gen_server:call(?MODULE, {reg, Pool, Pid}).

%% @doc 取池里的服务进程
get_server(Pool) ->
    gen_server:call(?MODULE, {get_pool_server, Pool}).

%% gen_server.
init([]) -> 
    {ok, #state{
        pools = [],
        map = #{}
    }}.

handle_call({reg, Pool, Pid}, _From, #state{pools = Pools, map = Map} = State) ->
    % 监控子进程
    NewMap = maps:merge(Map, #{erlang:monitor(process, Pid) => Pool}),
    NewPools = case lists:keytake(Pool, #manager_info.pool_id, Pools) of
        {value, #manager_info{pids = Pids} = Info, RestPools} ->
            NewInfo = Info#manager_info{pids = [Pid|Pids]},
            [NewInfo|RestPools];
        false ->
            NewInfo = #manager_info{pool_id = Pool, index = 1, pids = [Pid]},
            [NewInfo|Pools]
    end,
    {reply, ok, State#state{pools = NewPools, map = NewMap}};
handle_call({get_pool_server, Pool}, _From, #state{pools = Pools} = State) ->
    case lists:keytake(Pool, #manager_info.pool_id, Pools) of
        {value, #manager_info{index = Index, pids = Pids} = Info, NewPools} ->
            Max = length(Pids),
            ValidIndex = case Index > Max of
                true -> 1;
                false -> Index
            end,
            Pid = lists:nth(ValidIndex, Pids),
            NewInfo = Info#manager_info{index = ValidIndex + 1},
            {reply, Pid, State#state{pools = [NewInfo|NewPools]}};
        false ->
            {reply, undefined, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, process, Object, _Info}, #state{pools = Pools, map = Map} = State) ->
    {Pool, NewMap} = maps:take(MonitorRef, Map),
    case lists:keytake(Pool, #manager_info.pool_id, Pools) of
        {value, #manager_info{pids = [Object]}, NewPools} ->
            % 子进程都关闭了，重启池
            mysql_driver_sup:restart(Pool),
            {noreply, State#state{pools = NewPools, map = NewMap}};
        {value, #manager_info{pids = Pids} = Info, NewPools} ->
            NewInfo = Info#manager_info{pids = lists:delete(Object, Pids)},
            lager:warning("Pool ~p down one connect!", [Pool]),
            {noreply, State#state{pools = [NewInfo|NewPools], map = NewMap}};
        false ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
