%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_route).

-include("mysql.hrl").

%% API.
-export([
    routing/4
]).

%% @doc handle mysql command
routing(query_sql, Capability, #mysql_handle{
        packet = #mysql_packet{buff = Buff}
    } = Handle, Data) ->
    case mysql_packet:decode(Data, Buff, Capability) of
        {Res, <<>>} ->
            handle_res(Res, Handle, Capability);
        {Res, NewData} ->
            case handle_res(Res, Handle, Capability) of
                {_, #mysql_handle{} = NewHandle} ->
                    routing(query_sql, Capability, NewHandle, NewData);
                Error ->
                    Error
            end
    end.

handle_res(#mysql_packet{payload = false} = Packet, Handle, _Capability) ->
    {need_more, Handle#mysql_handle{packet = Packet}};
handle_res(#mysql_packet{payload = true} = Packet, #mysql_handle{
        step = Step,
        packet = #mysql_packet{sequence_id = OldIndex}
    } = Handle, Capability) ->
    #mysql_handle{
        result = #mysql_result{is_reply = IsReply}
    } = NewHandle = do_routing(Handle#mysql_handle{
        packet = Packet
    }, Step, OldIndex, Capability),
    case IsReply == 0 of
        true ->
            {continue, NewHandle#mysql_handle{packet = Packet#mysql_packet{buff = <<>>}}};
        false ->
            {ok, NewHandle#mysql_handle{packet = Packet#mysql_packet{buff = <<>>}}}
    end;
handle_res(#mysql_packet{payload = #mysql_ok_packet{}}, _Handle, _Capability) ->
    {error, ok};
handle_res(#mysql_packet{payload = #mysql_err_packet{code = Code, message = Msg}}, _Handle, _Capability) ->
    {error, {Code, Msg}};
handle_res(#mysql_packet{payload = #mysql_eof_packet{}} = Packet, Handle, _Capability) ->
    {ok, Handle#mysql_handle{packet = Packet#mysql_packet{payload = undefined, buff = <<>>}}};
handle_res(#mysql_packet{payload = Payload}, _Handle, _Capability) ->
    {error, Payload}.

do_routing(#mysql_handle{
        packet = #mysql_packet{
            sequence_id = Index,
            buff = Data
        },
        result = #mysql_result{
            result = Result
        } = ResultInfo
    } = Handle, Step, OldIndex, Capability) when (Index - OldIndex) == 1 ->
    case mysql_command:decode(Data, Capability, Step) of
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
