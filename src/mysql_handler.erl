%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_handler).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([
	start_link/1, 
	call/3, call/4
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
	socket = 0,
	connect_id = 0,
	capability = 0,
	flag = init, 		% init | handshake | ready | commond_event() :: query_sql
	handle,  			% undefined | #mysql_handle{}
	cache
}).

%%====================================================================
%% API functions
%%====================================================================

start_link(Opts) ->
	gen_server:start_link(?MODULE, Opts, []).

%% @doc send the sentence to process and wait for reply
call(Pid, Event, Bin) ->
	call(Pid, Event, Bin, 5000).
call(Pid, _Event, _Bin, _Timeout) when Pid == self() ->
	{error, call_to_self};
call(Pid, Event, Bin, Timeout) ->
	gen_server:call(Pid, {Event, Bin}, Timeout).
		
%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
	case mysql_connect:start(Opts) of
		{ok, ConnectId, Socket, Capability} ->
			{ok, #state{
				socket = Socket,
				connect_id = ConnectId,
				capability = Capability,
				flag = ready,
				cache = queue:new()
			}};
		Error ->
			error_logger:error_msg("Start mysql connect fail:~p", [Error]),
			{stop, normal}
	end.

handle_call({query_sql, String}, From, #state{
	socket = Socket,
	capability = Capability,
	cache = Cache,
	flag = ready
} = State) ->
	UpdateCache = cache(Cache, query_sql, From, String),
	{Handle, NewCache} = get_cache(UpdateCache),
	case do_handle(Socket, Capability, Handle) of
    	{ok, #mysql_handle{flag = Flag} = NewHandle} ->
    		{noreply, State#state{flag = Flag, handle = NewHandle, cache = NewCache}};
    	_ ->
    		{noreply, State#state{cache = NewCache}}
    end;
handle_call({query_sql, String}, From, #state{cache = Cache} = State) ->
	NewCache = cache(Cache, query_sql, From, String),
	{noreply, State#state{cache = NewCache}};

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(next, #state{
	socket = Socket,
	capability = Capability,
	cache = Cache,
	flag = ready
} = State) ->
	{Handle, NewCache} = get_cache(Cache),
	case do_handle(Socket, Capability, Handle) of
    	{ok, #mysql_handle{flag = Flag} = NewHandle} ->
    		{noreply, State#state{flag = Flag, handle = NewHandle, cache = NewCache}};
    	empty ->
    		{noreply, State#state{cache = NewCache}};
    	_ ->
    		next(),
    		{noreply, State#state{cache = NewCache}}
    end;
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({tcp, Socket, Data}, State = #state{
		socket = Socket,
		capability = Capability,
		handle = Handle
	}) ->
	NewState = case mysql_route:routing(Handle, Capability, Data) of
		{ok, NewHandle} ->
			next(State#state{handle = NewHandle});
		{need_more, NewHandle} ->
			inet:setopts(Socket, [{active, once}]),
			State#state{handle = NewHandle};
		{continue, NewHandle} ->
			next(State#state{handle = NewHandle});
		{error, NewHandle} ->
			next(State#state{handle = NewHandle})
	end,
	{noreply, NewState};
handle_info({tcp_closed, Socket}, State = #state{socket = Socket}) ->
	{stop, normal, State};
handle_info({tcp_error, Socket, Reason}, State = #state{socket = Socket}) ->
	{stop, Reason, State};
handle_info(Info, State) ->
	error_logger:info_msg("unknown info msg:~p", [Info]),
	{noreply, State}.

terminate(_Reason, #state{socket = 0}) ->
	ok;
terminate(_Reason, #state{socket = Socket}) ->
	gen_tcp:close(Socket),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%====================================================================
%% internal functions
%%====================================================================

%% 缓存指令
cache(Cache, Event, From, String) ->
	Handle = #mysql_handle{
		msg = String,
		from = From,
		flag = Event
    },
    queue:in(Handle, Cache).

%% 取指令(先进先出)
get_cache(Cache) ->
	case queue:out(Cache) of
		{{value, #mysql_handle{flag = query_sql} = Handle}, NewCache} ->
			%% TODO 目前仅处理查询
			{Handle#mysql_handle{
				packet = #mysql_packet{},
				result = #mysql_result{}
			}, NewCache};
		{empty, NewCache} ->
			{empty, NewCache}
	end.   

%% 下一步操作
next(#state{
	handle = #mysql_handle{
		packet = #mysql_packet{sequence_id = Index},
		result = #mysql_result{
			result = #mysql_local_infile{filename = Filename},
			reply = ReplyList
		} = ResultInfo
	} = Handle,
	socket = Socket
} = State) ->
	{ok, FileContent} = file:read_file(Filename),
	SendBin = mysql_command:encode(?COM_TEXT, FileContent, Index + 1),
	case do_send(Socket, SendBin) of
		ok ->
			State;
		Error ->
			next(#state{
				handle = Handle#mysql_handle{
					result = ResultInfo#mysql_result{
						is_reply = true,
						result = undefined,
						reply = [Error|ReplyList]
					}
				}
			})
	end;
next(#state{capability = Capability, handle = Handle} = State) ->
	reply_result(Capability, Handle),
	%% 触发执行缓存内容
	next(),
	State#state{flag = ready, handle = undefined}.

%% 触发执行缓存中的下一个指令
next() ->
	gen_server:cast(self(), next).

%% 发送指令
do_handle(Socket, Capability, #mysql_handle{msg = String} = Handle) ->
	SendBin = mysql_command:encode(?COM_QUERY, String, 0),
	case do_send(Socket, SendBin) of
		ok ->
			{ok, Handle#mysql_handle{flag = query_sql}};
		Error ->
			reply_result(Capability, Handle#mysql_handle{
				result = #mysql_result{is_reply = true, result = undefined, reply = [Error]}
			}),
			Error
	end;
do_handle(_Socket, _Capability, _Handle) ->
	empty.

do_send(Socket, Bin) ->
	inet:setopts(Socket, [{active, once}]),
	gen_tcp:send(Socket, Bin).

%% 回复调用者
reply_result(Capability, #mysql_handle{
	result = #mysql_result{is_reply = true, reply = Result},
	from = From
}) when From =/= undefined ->
	gen_server:reply(From, mysql_result:translate(Capability, Result)),
	ok;
reply_result(_Capability, _Handle) ->
	skip.
