%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_text_result).

-include("mysql.hrl").

-export([
    decode/3,
    mutil/2
]).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc text result set
decode(<<251:8, Filename/binary>> = Data, Capability, undefined) ->
    case ?CLIENT_LOCAL_FILES == ?CLIENT_LOCAL_FILES band Capability of
        true ->
            %% TODO 不准确的区分，建议不要使用该功能
            #mysql_local_infile{filename = Filename};
        false ->
            decode(Data, Capability, init(Capability))
    end;
decode(Data, Capability, undefined) ->
    decode(Data, Capability, init(Capability));
decode(Data, Capability, #mysql_query{} = Query) ->
    Res = mysql_command:decode(Query#mysql_query{
        columns = [], rows = []
    }, Data, Capability),
    decode_text(Res, Query).

%% @doc 多结果处理
mutil(#mysql_result{
    result = #mysql_query{} = Query,
    reply = ReplyList
} = ResultInfo, StatusFlags) ->
    case is_mutil(StatusFlags) of
        true ->
            {need_more, ResultInfo#mysql_result{
                result = undefined,
                reply = [Query|ReplyList]
            }};
        false ->
            {ok, ResultInfo#mysql_result{
                is_reply = true,
                result = undefined,
                reply = lists:reverse([Query|ReplyList])
            }}
    end;
mutil(#mysql_result{
    reply = ReplyList
} = ResultInfo, StatusFlags) ->
    case is_mutil(StatusFlags) of
        true ->
            {need_more, ResultInfo#mysql_result{
                is_reply = false,
                reply = lists:reverse(ReplyList)
            }};
        false ->
            {ok, ResultInfo}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% 初始化查询
init(Capability) ->
    MetadataMark =
        case ?CLIENT_OPTIONAL_RESULTSET_METADATA == ?CLIENT_OPTIONAL_RESULTSET_METADATA band Capability of
            true ->
                ?RESULTSET_METADATA_NONE;
            false ->
                ?RESULTSET_METADATA_FULL
        end,
    #mysql_query{
        is_eof = ?CLIENT_DEPRECATE_EOF =/= ?CLIENT_DEPRECATE_EOF band Capability,
        metadata_mark = MetadataMark
    }.

%% 解析text系列包
decode_text({column_count, ColumnCount, MetadataMark}, #mysql_query{
    step = 0,
    is_eof = IsEof
} = Query) ->
    Step =
        case MetadataMark == ?RESULTSET_METADATA_NONE andalso (not IsEof) of
            true -> 2;
            false -> 1
        end,
    Query#mysql_query{
        step = Step,
        metadata_mark = MetadataMark,
        column = ColumnCount
    };
decode_text({column_definition, ColumnDefinition}, #mysql_query{
    step = 1,
    is_eof = true,
    columns = Columns
} = Query) ->
    Query#mysql_query{columns = [ColumnDefinition|Columns]};
decode_text({column_definition, ColumnDefinition}, #mysql_query{
    step = 1,
    column = Num,
    columns = Columns
} = Query) when Num > 1 ->
    Query#mysql_query{column = Num - 1, columns = [ColumnDefinition|Columns]};
decode_text({column_definition, ColumnDefinition}, #mysql_query{
    step = 1,
    columns = Columns
} = Query) ->
    Query#mysql_query{step = 2, column = 0,
        columns = lists:reverse([ColumnDefinition|Columns])};
decode_text({row, RowInfo}, #mysql_query{
    step = 2,
    rows = Rows
} = Query) ->
    Query#mysql_query{rows = [RowInfo|Rows]}.

%% 是否多结果
is_mutil(StatusFlags) ->
    ?SERVER_MORE_RESULTS_EXISTS == ?SERVER_MORE_RESULTS_EXISTS band StatusFlags.
