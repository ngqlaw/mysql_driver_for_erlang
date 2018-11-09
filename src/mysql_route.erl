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
                {continue, #mysql_handle{} = NewHandle} ->
                    routing(query_sql, Capability, NewHandle, NewData);
                Reply ->
                    Reply
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
handle_res(#mysql_packet{payload = #mysql_eof_packet{
        warnings = Warnings,
        status_flags = StatusFlags
    }} = Packet, #mysql_handle{
        step = 2,
        result = #mysql_result{
            result = Result
        } = ResultInfo
    } = Handle, _Capability) ->
    NewResultInfo = ResultInfo#mysql_result{
        result = [{eof, Warnings, StatusFlags}|Result]
    },
    {continue, Handle#mysql_handle{
        step = 3,
        packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
        result = NewResultInfo
    }};
handle_res(#mysql_packet{payload = #mysql_eof_packet{
        warnings = Warnings,
        status_flags = StatusFlags
    }} = Packet, #mysql_handle{
        result = #mysql_result{
            result = Result
        } = ResultInfo
    } = Handle, _Capability) ->
    case ?SERVER_MORE_RESULTS_EXISTS == ?SERVER_MORE_RESULTS_EXISTS band StatusFlags of
        true ->
            NewResultInfo = ResultInfo#mysql_result{
                result = [{eof, Warnings, StatusFlags}|Result]
            },
            {continue, Handle#mysql_handle{
                step = 0,
                packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
                result = NewResultInfo
            }};
        false ->
            NewResultInfo = ResultInfo#mysql_result{
                is_reply = 1,
                result = lists:reverse([{eof, Warnings, StatusFlags}|Result])
            },
            {ok, Handle#mysql_handle{
                packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
                result = NewResultInfo
            }}
    end;
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
        Other ->
            NewResultInfo = ResultInfo#mysql_result{
                is_reply = 1,
                result = lists:reverse([Other|Result])
            },
            Handle#mysql_handle{result = NewResultInfo}
    end.
