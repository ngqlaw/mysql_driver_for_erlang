%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_handler).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([
	start_link/2, 
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
	flag = init, 		% init | handshake | ready | work
	handle,  			% undefined | #mysql_handle{}
	cache
}).

-define(HANDSHAKE, 0).

%% API.
start_link(Args, Pool) ->
	gen_server:start_link(?MODULE, [Pool, Args], []).

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
init([Pool, Opts]) ->
	case mysql_connect:connect(Opts) of
		{ok, Socket} ->
			{ok, #state{
				pool = Pool,
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
    	{ok, NewHandle} ->
    		{noreply, State#state{flag = work, handle = NewHandle, cache = NewCache}};
    	_ ->
    		{noreply, State#state{cache = NewCache}}
    end;
handle_cast({query_sql, {Pid, String}}, #state{cache = Cache} = State) ->
	NewCache = cache(Cache, query_sql, Pid, String),
	{noreply, State#state{cache = NewCache}};
handle_cast(next, #state{socket = Socket, cache = Cache, flag = ready} = State) ->
	{Handle, NewCache} = get_cache(Cache),
	case do_handle(Socket, Handle) of
    	{ok, NewHandle} ->
    		{noreply, State#state{flag = work, handle = NewHandle, cache = NewCache}};
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
    } = mysql_connect:handle_connect(Data, Opts),
	gen_tcp:send(Socket, ResponseBin),
	{noreply, State#state{
		flag = handshake,
		connect_id = ConnectID,
		version = Version,
		capability = Capability
	}};
handle_info({tcp, Socket, Data}, #state{
		pool = Pool,
		socket = Socket,
		flag = handshake,
		capability = Capability,
		handle = Opts
	} = State) ->
	case mysql_connect:handle_handshake(Data, Capability, Opts) of
		ok ->
			% 注册
			mysql_manager:reg(Pool, self()),
			{noreply, State#state{flag = ready, handle = undefined}};
		Error ->
			{stop, Error, State}
	end;
handle_info({tcp, Socket, Data}, State = #state{socket = Socket}) ->
	lager:info("receive reply:~p", [Data]),
	NewState = routing(State, Data),
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

routing(#state{
		capability = Capability, 
		handle = Handle
	} = State, Data) ->
	#mysql_handle{
		step = Step,
		packet = #mysql_packet{
			sequence_id = OldIndex,
			buff = Buff
		}
	} = Handle,
	case mysql_packet:packet(Data, Buff, Capability) of
		#mysql_packet{payload = false} = NewPacket ->
			State#state{handle = Handle#mysql_handle{packet = NewPacket}};
		#mysql_packet{payload = true} = NewPacket ->
			#mysql_handle{
				result = #mysql_result{is_reply = IsReply}
			} = NewHandle = do_routing(Handle#mysql_handle{
				packet = NewPacket
			}, Step, OldIndex, Capability),
			case IsReply == 0 of
				true ->
					State#state{handle = NewHandle};
				false ->
					reply_result(NewHandle),
					% 触发执行缓存内容
					next(),
					State#state{flag = ready, handle = undefined}
			end;
		#mysql_packet{sequence_id = _Index, payload = Payload} ->
			reply_result(Handle#mysql_handle{result = #mysql_result{result = Payload}}),
			% 触发执行缓存内容
			next(),
			State#state{flag = ready, handle = undefined};
		Error ->
			lager:error("handle request fail:~p", [Error]),
			reply_result(Handle#mysql_handle{result = #mysql_result{result = Error}}),
			% 触发执行缓存内容
			next(),
			State#state{flag = ready, handle = undefined}
	end.

do_routing(#mysql_handle{
		packet = #mysql_packet{
			sequence_id = Index,
			buff = Data
		},
		result = #mysql_result{
			result = Result
		} = ResultInfo
	} = Handle, Step, OldIndex, Capability) when (Index - OldIndex) == 1 ->
	case mysql_packet:query_packet(Data, Capability, Step) of
		{column_count, ColumnCount} ->
			NewResultInfo = ResultInfo#mysql_result{
				result = [{column, ColumnCount, 0, []}|Result]
			},
			Handle#mysql_handle{step = 1, result = NewResultInfo};
		{column_definition, ColumnDefinition} ->
			[{column, ColumnCount, Num, Columns}|T] = Result,
			NewNum = Num + 1,
			case NewNum == ColumnCount of
				true ->
					NewStep = 
					case ?CLIENT_DEPRECATE_EOF =/= ?CLIENT_DEPRECATE_EOF band Capability of
						true -> 2;
						false -> 3
					end,
					NewResultInfo = ResultInfo#mysql_result{
						result = [{column, ColumnCount, [ColumnDefinition|Columns]}|T]
					},
					Handle#mysql_handle{step = NewStep, result = NewResultInfo};
				false ->
					NewResultInfo = ResultInfo#mysql_result{
						result = [{column, ColumnCount, NewNum, [ColumnDefinition|Columns]}|T]
					},
					Handle#mysql_handle{result = NewResultInfo}
			end;
		{row, RowInfo} ->
			NewResultInfo = ResultInfo#mysql_result{
				result = [{row, RowInfo}|Result]
			},
			Handle#mysql_handle{result = NewResultInfo};
		{eof, EOF} when Step == 2 ->
			NewResultInfo = ResultInfo#mysql_result{
				result = [{eof, EOF}|Result]
			},
			Handle#mysql_handle{step = 3, result = NewResultInfo};
		{eof, [_Warnings, StatusFlags] = EOF} when Step == 4 ->
			case ?SERVER_MORE_RESULTS_EXISTS == ?SERVER_MORE_RESULTS_EXISTS band StatusFlags of
				true ->
					NewResultInfo = ResultInfo#mysql_result{
						result = [{eof, EOF}|Result]
					},
					Handle#mysql_handle{step = 0, result = NewResultInfo};
				false ->
					NewResultInfo = ResultInfo#mysql_result{
						is_reply = 1,
						result = [{eof, EOF}|Result]
					},
					Handle#mysql_handle{result = NewResultInfo}
			end;
		Other ->
			NewResultInfo = ResultInfo#mysql_result{
				is_reply = 1,
				result = [Other|Result]
			},
			Handle#mysql_handle{result = NewResultInfo}
	end.

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

get_cache(Cache) ->
	case queue:out(Cache) of
		{{value, Handle}, NewCache} ->
			{Handle, NewCache};
		{empty, NewCache} ->
			{empty, NewCache}
	end.   

next() ->
	gen_server:cast(self(), next).

do_handle(Socket, #mysql_handle{msg = String} = Handle) ->
	SendBin = mysql_packet:query_request(String, 0),
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

reply_result(#mysql_handle{result = #mysql_result{result = Result}, pid = Pids}) ->
	[Pid ! {ok, Result} || Pid <- Pids, Pid =/= undefined],
	ok.