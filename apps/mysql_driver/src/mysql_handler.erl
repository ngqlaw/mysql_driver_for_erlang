%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_handler).

-include("mysql.hrl").

-behaviour(gen_server).

%% API.
-export([
		start_link/1, 
		send/3
		]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(mysql_handle, {
		msg = <<>>, 
		pid = [],
		index = 0, 
		step = 0,
		flag = query, 	%for now only support:query 
		result = [],
		is_reply = 0,	%0 | 1 <==> not reply | reply
		bin = <<>>
		}).

-record(state, {
		socket = 0, 
		connect_id = 0, 
		version = 10, 
		caps = 0, 
		handle = free % free | #mysql_handle{}
		}).

-define(HANDSHAKE, 0).

%% API.
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

%% @doc send the sentence to process
send(Pid, Event, Bin) ->
	Self = self(),
	gen_server:cast(Pid, {Event, {Self, Bin}}),
	case Self == Pid of
		true ->
			skip;
		false ->
			receive
				{ok, Result} ->
					Result
			after 3000 ->
				{error, time_out}
			end
	end.
		
%% gen_server.
init([Socket, ConnectID, Version, Caps]) -> 
	{ok, #state{
		socket = Socket, 
		connect_id = ConnectID, 
		version = Version, 
		caps = Caps, 
		handle = free}
	}.

handle_info({tcp, Socket, Data}, State = #state{socket = Socket, handle = Handle}) ->
	%io:format("receive bin:~p~n", [Data]),
	#mysql_handle{bin = OldBin} = Handle,
	Bin = <<OldBin/binary, Data/binary>>,
	routing(State#state{handle = Handle#mysql_handle{bin = Bin}});
handle_info({tcp_closed, Socket}, State = #state{socket = Socket}) ->
	io:format("stop!~n", []),
	{stop, normal, State};
handle_info({tcp_error, Socket, Reason}, State = #state{socket = Socket}) ->
	io:format("error:~p!~n", [Reason]),
	{stop, Reason, State};
handle_info(Info, State) ->
	io:format("receive other info:~p~n", [Info]),
	{noreply, State}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({query, {Pid, String}}, State) ->
	%io:format("get handle query:~p~n", [String]),
	%% @TODO control the execute,make sure one is done!
	SendBin = mysql_util:query_request(String, 0),
	Handle = 
	#mysql_handle{msg = String, 
		pid = [Pid],
		index = 0, 
		step = 0,
		flag = query,
		result = [],
		is_reply = 0,
		bin = <<>>},
	do_send(State#state{handle = Handle}, SendBin);
handle_cast(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, #state{socket = Socket, connect_id = ID}) ->
	gen_tcp:close(Socket),
	ets:delete(mysql_conn, ID),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

routing(#state{
	caps = Capability, 
	handle = #mysql_handle{
		bin = <<Len:24/little, Index:8, Data/binary>>, 
		index = OldIndex
		} = Handle
	} = State) ->
	case byte_size(Data) >= Len of
		true ->
			{Bin, RestBin} = erlang:split_binary(Data, Len),
			#mysql_handle{is_reply = IsReply} = NewHandle = 
			do_routing(Handle#mysql_handle{bin = RestBin, index = Index}, Index - OldIndex, Capability, Bin),
			%io:format("handle info:~p~n", [NewHandle]),
			case RestBin == <<>> of
				true ->
					case IsReply == 0 of
						true ->
							{noreply, State#state{handle = NewHandle}};
						false ->
							reply_result(NewHandle),
							{noreply, State#state{handle = free}}
					end;
				false ->
					routing(State#state{handle = NewHandle})
			end;
		false ->
			{noreply, State}
	end.
	
do_routing(#mysql_handle{flag = query, step = Step, result = Result} = Handle, 1, Capability, Bin) ->
	case mysql_util:parser_query(Bin, Capability, Step) of
		{column_count, ColumnCount} ->
			Handle#mysql_handle{step = 1, result = [{column, ColumnCount, 0, []}|Result]};
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
					Handle#mysql_handle{step = NewStep, result = [{column, ColumnCount, [ColumnDefinition|Columns]}|T]};
				false ->
					Handle#mysql_handle{result = [{column, ColumnCount, NewNum, [ColumnDefinition|Columns]}|T]}
			end;
		{row, RowInfo} ->
			%io:format("info3:~p~n", [RowInfo]),
			Handle#mysql_handle{result = [{row, RowInfo}|Result]};
		{eof, EOF} when Step == 2 ->
			Handle#mysql_handle{step = 3, result = [{eof, EOF}|Result]};
		{eof, [_Warnings, StatusFlags] = EOF} when Step == 4 ->
			case ?SERVER_MORE_RESULTS_EXISTS == ?SERVER_MORE_RESULTS_EXISTS band StatusFlags of
				true ->
					Handle#mysql_handle{step = 0, result = [{eof, EOF}|Result]};
				false ->
					Handle#mysql_handle{result = [{eof, EOF}|Result], is_reply = 1}
			end;
		Other ->
			Handle#mysql_handle{result = [Other|Result], is_reply = 1}
	end;
do_routing(#mysql_handle{result = Result} = Handle, DIndex, _Capability, _Bin) when DIndex =/= 1 ->
	Handle#mysql_handle{result = [{error, receive_wrong_packet_order}|Result]};
do_routing(#mysql_handle{result = Result} = Handle, _DIndex, _Capability, _Bin) ->
	Handle#mysql_handle{result = [{error, unknown_method}|Result]}.
	
do_send(State = #state{socket = Socket}, Bin) ->
	case gen_tcp:send(Socket, Bin) of
		ok ->
			{noreply, State};
		Error ->
			{stop, Error, State}
	end.

reply_result(#mysql_handle{result = Result, pid = Pids}) ->
	[Pid ! {ok, Result} || Pid <- Pids],
	ok.