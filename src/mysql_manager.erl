%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_manager).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([
    start_link/1,
    reg/3,
    get_server/1,
    get_all/1
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
    pool,
    sup_ref,
    opts = [],
    index = 1,
    pids = []
}).

%%====================================================================
%% API functions
%%====================================================================

start_link(Pool) ->
    gen_server:start_link(?MODULE, [Pool], []).

%% @doc 注册进程
reg(ManagerPid, Pid, InitFun) when is_pid(ManagerPid) ->
    gen_server:cast(ManagerPid, {reg, Pid, InitFun});
reg(undefined, _Pid, _InitFun) -> false;
reg(Pool, Pid, InitFun) ->
    ManagerPid = mysql_pool_manager:get_server(Pool),
    reg(ManagerPid, Pid, InitFun).

%% @doc 取池里的服务进程
get_server(ManagerPid) when is_pid(ManagerPid) ->
    gen_server:call(ManagerPid, get_server);
get_server(undefined) -> undefined;
get_server(Pool) ->
    ManagerPid = mysql_pool_manager:get_server(Pool),
    get_server(ManagerPid).

%% @doc 获取池所有进程
get_all(ManagerPid) when is_pid(ManagerPid) ->
    gen_server:call(ManagerPid, get_all);
get_all(undefined) -> [];
get_all(Pool) ->
    ManagerPid = mysql_pool_manager:get_server(Pool),
    get_all(ManagerPid).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Pool]) ->
    % 注册
    mysql_pool_manager:reg(Pool, self()),
    {ok, #state{
        pool = Pool,
        index = 1,
        pids = []
    }}.

handle_call(get_server, _From, #state{index = Index, pids = [_|_] = Pids} = State) ->
    Max = length(Pids),
    ValidIndex = case Index > Max of
        true -> 1;
        false -> Index
    end,
    Pid = lists:nth(ValidIndex, Pids),
    {reply, Pid, State#state{index = ValidIndex + 1}};
handle_call(get_server, _From, #state{} = State) ->
    {reply, undefined, State};
handle_call(get_all, _From, #state{pids = Pids} = State) ->
    {reply, Pids, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({reg, Pid, InitFun}, #state{pids = Pids} = State) ->
    case InitFun(Pid) of
        ok ->
            % 监控子进程
            erlang:monitor(process, Pid),
            {noreply, State#state{pids = [Pid|Pids]}};
        _ ->
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Object, _Info}, #state{pool = Pool, pids = Pids} = State) ->
    case lists:delete(Object, Pids) of
        Pids ->
            {noreply, State};
        NewPids ->
            lager:warning("Pool ~p down one connect!", [Pool]),
            {noreply, State#state{pids = NewPids}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
