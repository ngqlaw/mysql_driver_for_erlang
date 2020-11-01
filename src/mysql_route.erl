%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_route).

-include("mysql.hrl").

%% API.
-export([
    routing/3
]).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc handle mysql command,handshake
routing(#mysql_handle{
        flag = Flag,
        packet = #mysql_packet{buff = Buff}
    } = Handle, Capability, Data) ->
    case mysql_packet:decode(Data, Buff, Capability, Flag) of
        {Res, <<>>} ->
            handle_res(Res, Handle, Capability);
        {Res, NewData} ->
            case handle_res(Res, Handle, Capability) of
                {continue, #mysql_handle{} = NewHandle} ->
                    routing(NewHandle, Capability, NewData);
                Reply ->
                    Reply
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% need more
handle_res(#mysql_packet{payload = false} = Packet, Handle, _Capability) ->
    {need_more, Handle#mysql_handle{packet = Packet}};
%% unknown packet
handle_res(#mysql_packet{
    sequence_id = Index, payload = unknown_packet_form, buff = Data
} = Packet, #mysql_handle{
    packet = #mysql_packet{sequence_id = OldIndex},
    result = #mysql_result{result = Result} = ResultInfo
} = Handle, Capability) when Index == OldIndex + 1 ->
    %% common string
    NewResult = mysql_text_result:decode(Data, Capability, Result),
    {continue, Handle#mysql_handle{
        packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
        result = ResultInfo#mysql_result{result = NewResult}
    }};
handle_res(#mysql_packet{payload = unknown_packet_form} = Packet, Handle, _Capability) ->
    %% really unknown!!!
    {error, reply(Packet, Handle)};
%% ok
handle_res(#mysql_packet{
    payload = #mysql_ok_packet{status_flags = StatusFlags}
} = Packet, #mysql_handle{
    result = #mysql_result{
        result = #mysql_query{step = 2, is_eof = false}
    } = ResultInfo
} = Handle, _Capability) ->
    {Mark, NewResultInfo} = mysql_text_result:mutil(ResultInfo, StatusFlags),
    {Mark, Handle#mysql_handle{
        packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
        result = NewResultInfo
    }};
handle_res(#mysql_packet{
    payload = #mysql_ok_packet{status_flags = StatusFlags}
} = Packet, Handle, _Capability) ->
    #mysql_handle{
        result = #mysql_result{} = ResultInfo
    } = TempHandle = reply(Packet, Handle),
    {Mark, NewResultInfo} = mysql_text_result:mutil(ResultInfo, StatusFlags),
    {Mark, TempHandle#mysql_handle{result = NewResultInfo}};
%% auth more
handle_res(#mysql_packet{payload = #mysql_auth_more{}} = Packet, Handle, _Capability) ->
    {continue, Handle#mysql_handle{packet = Packet}};
%% error
handle_res(#mysql_packet{
    payload = #mysql_err_packet{}
} = Packet, Handle, _Capability) ->
    {error, reply(Packet, Handle)};
%% eof
handle_res(#mysql_packet{
    payload = #mysql_eof_packet{}
} = Packet, #mysql_handle{
    result = #mysql_result{
        result = #mysql_query{step = 1, is_eof = true, columns = Columns} = Query
    } = ResultInfo
} = Handle, _Capability) ->
    NewResultInfo = ResultInfo#mysql_result{
        result = Query#mysql_query{step = 2, columns = lists:reverse(Columns)} 
    },
    {continue, Handle#mysql_handle{
        packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
        result = NewResultInfo
    }};
handle_res(#mysql_packet{
    payload = #mysql_eof_packet{status_flags = StatusFlags}
} = Packet, #mysql_handle{
    result = #mysql_result{
        result = #mysql_query{step = 2, is_eof = true}
    } = ResultInfo
} = Handle, _Capability) ->
    {Mark, NewResultInfo} = mysql_text_result:mutil(ResultInfo, StatusFlags),
    {Mark, Handle#mysql_handle{
        packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
        result = NewResultInfo
    }};
handle_res(#mysql_packet{} = Packet, Handle, _Capability) ->
    {error, reply(Packet, Handle)}.

%% 返回
reply(#mysql_packet{
    payload = Payload
} = Packet, #mysql_handle{
    result = #mysql_result{reply = ReplyList} = ResultInfo
} = Handle) ->
    Handle#mysql_handle{
        packet = Packet#mysql_packet{payload = undefined, buff = <<>>},
        result = ResultInfo#mysql_result{
            is_reply = true,
            result = undefined,
            reply = lists:reverse([Payload|ReplyList])
        }
    }.
