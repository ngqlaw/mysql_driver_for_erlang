%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_route).

-include("mysql.hrl").

%% API.
-export([
    routing/4
]).

%% @doc handle mysql command
routing(query_sql, Capability, Handle, Data) ->
    #mysql_handle{
        step = Step,
        packet = #mysql_packet{
            sequence_id = OldIndex,
            buff = Buff
        }
    } = Handle,
    case mysql_packet:decode(Data, Buff, Capability) of
        #mysql_packet{payload = false} = NewPacket ->
            {need_more, Handle#mysql_handle{packet = NewPacket}};
        #mysql_packet{payload = true} = NewPacket ->
            #mysql_handle{
                result = #mysql_result{is_reply = IsReply}
            } = NewHandle = do_routing(Handle#mysql_handle{
                packet = NewPacket
            }, Step, OldIndex, Capability),
            case IsReply == 0 of
                true ->
                    {continue, NewHandle};
                false ->
                    {ok, NewHandle}
            end;
        #mysql_packet{payload = #mysql_ok_packet{}} ->
            {error, ok};
        #mysql_packet{payload = #mysql_err_packet{code = Code, message = Msg}} ->
            {error, {Code, Msg}};
        #mysql_packet{payload = Payload} ->
            {error, Payload}
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
