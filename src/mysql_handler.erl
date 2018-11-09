%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_handler).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([
	start_link/3, 
	call/3, call/4,
	cast/3
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
	socket = 0,
	connect_id = 0,
	version = 10,
	capability = 0,
	flag = init, 		% init | handshake | ready | commond_event() :: query_sql
	handle,  			% undefined | #mysql_handle{}
	cache
}).

%% API.
start_link(Args, SendPid, Pool) ->
	gen_server:start_link(?MODULE, [SendPid, Pool, Args], []).

%% @doc send the sentence to process and wait for reply
call(Pid, Event, Bin) ->
	call(Pid, Event, Bin, 5000).
call(Pid, _Event, _Bin, _Timeout) when Pid == self() ->
	{error, call_to_self};
call(Pid, Event, Bin, Timeout) ->
	gen_server:cast(Pid, {Event, {self(), Bin}}),
	receive
		{ok, Result} ->
			Result
	after Timeout ->
		{error, time_out}
	end.

%% @doc send the sentence to process
cast(Pid, Event, Bin) ->
	gen_server:cast(Pid, {Event, {undefined, Bin}}).
		
%% gen_server.
init([SendPid, Pool, Opts]) ->
	case mysql_connect:start(Opts) of
		{ok, Socket} ->
			{ok, #state{
				pool = {Pool, SendPid},
				socket = Socket,
				flag = init,
				handle = Opts,
				cache = queue:new()
			}};
		Error ->
			{stop, Error}
	end.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({query_sql, {Pid, String}}, #state{
		socket = Socket,
		cache = Cache,
		flag = ready
	} = State) ->
	UpdateCache = cache(Cache, query_sql, Pid, String),
	{Handle, NewCache} = get_cache(UpdateCache),
	case do_handle(Socket, Handle) of
    	{ok, #mysql_handle{flag = Flag} = NewHandle} ->
    		{noreply, State#state{flag = Flag, handle = NewHandle, cache = NewCache}};
    	_ ->
    		{noreply, State#state{cache = NewCache}}
    end;
handle_cast({query_sql, {Pid, String}}, #state{cache = Cache} = State) ->
	NewCache = cache(Cache, query_sql, Pid, String),
	{noreply, State#state{cache = NewCache}};
handle_cast(next, #state{socket = Socket, cache = Cache, flag = ready} = State) ->
	{Handle, NewCache} = get_cache(Cache),
	case do_handle(Socket, Handle) of
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

handle_info({tcp, Socket, Data}, #state{socket = Socket, flag = init, handle = Opts} = State) ->
	#mysql_handshake_response{
		connect_id = ConnectID,
    	version = Version,
        capability = Capability,
        response = ResponseBin
    } = mysql_connect:handle_reply(Data, Opts),
	gen_tcp:send(Socket, ResponseBin),
	{noreply, State#state{
		flag = handshake,
		connect_id = ConnectID,
		version = Version,
		capability = Capability,
		handle = #mysql_handle{}
	}};
handle_info({tcp, Socket, Data}, #state{
		pool = {Pool, SendPid},
		socket = Socket,
		flag = handshake,
		capability = Capability,
		handle = Handle
	} = State) ->
	case mysql_handshake:response(Data, Capability, Handle) of
		ok ->
			% register
			mysql_manager:reg(Pool, self()),
			SendPid ! ok,
			{noreply, State#state{pool = Pool, flag = ready, handle = undefined}};
		{need_more, NewHandle} ->
			{noreply, State#state{handle = NewHandle}};
		{further, _} ->
			% Client and server possibly exchange further packets 
			% as required by the server authentication method for 
			% the user account the client is trying to authenticate against.
			% !!! TODO not suport now !!!
			{stop, need_further_exchange, State};
		Error ->
			{stop, Error, State}
	end;
handle_info({tcp, Socket, Data}, State = #state{
		socket = Socket,
		flag = Flag,
		capability = Capability, 
		handle = Handle
	}) ->
	NewState = case mysql_route:routing(Flag, Capability, Handle, Data) of
		{ok, NewHandle} ->
			reply_result(NewHandle),
			% 触发执行缓存内容
			next(),
			State#state{flag = ready, handle = undefined};
		{need_more, NewHandle} ->
			State#state{handle = NewHandle};
		{continue, NewHandle} ->
			State#state{handle = NewHandle};
		{error, Error} ->
			reply_result(Handle#mysql_handle{result = #mysql_result{result = Error}}),
			% 触发执行缓存内容
			next(),
			State#state{flag = ready, handle = undefined}
	end,
	{noreply, NewState};
handle_info({tcp_closed, Socket}, State = #state{socket = Socket}) ->
	{stop, normal, State};
handle_info({tcp_error, Socket, Reason}, State = #state{socket = Socket}) ->
	{stop, Reason, State};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, #state{socket = Socket}) ->
	gen_tcp:close(Socket),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% 缓存指令
cache(Cache, Event, Pid, String) ->
	Handle = #mysql_handle{
		msg = String,
		pid = [Pid],
		flag = Event,
		step = 0,
		packet = #mysql_packet{}, 
    	result = #mysql_result{}
    },
    queue:in(Handle, Cache).

%% 取指令(先进先出)
get_cache(Cache) ->
	case queue:out(Cache) of
		{{value, Handle}, NewCache} ->
			{Handle, NewCache};
		{empty, NewCache} ->
			{empty, NewCache}
	end.   

%% 触发执行缓存中的下一个指令
next() ->
	gen_server:cast(self(), next).

%% 发送指令
do_handle(Socket, #mysql_handle{msg = String} = Handle) ->
	SendBin = mysql_command:encode(String, 0),
	case do_send(Socket, SendBin) of
		ok ->
			{ok, Handle#mysql_handle{step = 0}};
		Error ->
			reply_result(Handle#mysql_handle{result = #mysql_result{result = Error}}),
			Error
	end;
do_handle(_Socket, _) ->
	empty.

do_send(Socket, Bin) ->
	gen_tcp:send(Socket, Bin).

%% 回复调用者
reply_result(#mysql_handle{result = #mysql_result{result = Result}, pid = Pids}) ->
	[Pid ! {ok, Result} || Pid <- Pids, Pid =/= undefined],
	ok.