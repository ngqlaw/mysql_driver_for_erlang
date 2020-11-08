%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_result).

-include("mysql.hrl").

%% API.
-export([translate/2]).

%%====================================================================
%% API functions
%%====================================================================
%% @ mysql返回结果解析
translate(Capability, Res) ->
    IsPro41 = ?CLIENT_PROTOCOL_41 == (Capability band ?CLIENT_PROTOCOL_41),
    [do_translate(IsPro41, R) || R <- Res].

%%====================================================================
%% internal functions
%%====================================================================
do_translate(true, #mysql_query{columns = Columns, rows = Rows}) ->
    ColumnsDef = lists:foldr(
        fun([<<"def">>, _schema, _Table, _OrgTable, _Name, OrgName, _, _, Type, Flag, _, _], Acc1) ->
            [{OrgName, Type}|Acc1]
        end, [], Columns),
    {ok, #{
        columns => [ColumnName || {ColumnName, _} <- ColumnsDef],
        rows => [value(ColumnsDef, Row) || Row <- Rows]
    }};
do_translate(fales, #mysql_query{columns = Columns, rows = Rows}) ->
    ColumnsDef = lists:foldr(
        fun([_Table, Name, _, Type, _, Flag, _|_], Acc1) ->
            [{Name, Type}|Acc1]
        end, [], Columns),
    {ok, #{
        columns => [ColumnName || {ColumnName, _} <- ColumnsDef],
        rows => [value(ColumnsDef, Row) || Row <- Rows]
    }};
do_translate(_IsPro41, #mysql_ok_packet{affected_rows = Affected}) ->
    {ok, Affected};
do_translate(_IsPro41, #mysql_err_packet{code = Code, message = Msg}) ->
    {error, Code, Msg};
do_translate(_IsPro41, Res) ->
    Res.

%% 转换为erlang数值
value([{_, ?MYSQL_TYPE_DECIMAL}|Def], [Value|T]) ->
    [binary_to_integer(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_TINY}|Def], [Value|T]) ->
    [binary_to_integer(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_SHORT}|Def], [Value|T]) ->
    [binary_to_integer(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_LONG}|Def], [Value|T]) ->
    [binary_to_integer(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_LONGLONG}|Def], [Value|T]) ->
    [binary_to_integer(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_INT24}|Def], [Value|T]) ->
    [binary_to_integer(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_FLOAT}|Def], [Value|T]) ->
    [binary_to_float(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_DOUBLE}|Def], [Value|T]) ->
    [binary_to_float(Value)|value(Def, T)];
value([{_, ?MYSQL_TYPE_NULL}|Def], [_Value|T]) ->
    [undefined|value(Def, T)];
value([_|Def], [Value|T]) ->
    [Value|value(Def, T)];
value([], []) -> [].
