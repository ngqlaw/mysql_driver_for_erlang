%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_command).

-include("mysql.hrl").

-export([
    encode/3,
    decode/3
]).

%% @doc 发送请求
encode(?COM_TEXT, Content, Index) when is_binary(Content) ->
    Len = byte_size(Content),
    <<Len:24/little, Index:8, Content/binary>>;
encode(COM, Content, Index) when is_binary(Content) ->
    Len = byte_size(Content) + 1,
    <<Len:24/little, Index:8, COM:8, Content/binary>>;
encode(COM, Content, Index) when is_list(Content) ->
    Bin = iolist_to_binary(Content),
    encode(COM, Bin, Index).

%% @doc for query result
decode(#mysql_query{step = 0, metadata_mark = ?RESULTSET_METADATA_NONE}, Packet, _Capability) ->
    {MetadataMark, Packet0} = mysql_util:parser_integer(Packet),
    case mysql_util:parser_integer(Packet0) of
        {ColumnCount, <<>>} ->
            {column_count, ColumnCount, MetadataMark};
        {_, _} ->
            {error, unknown_packet_form}
    end;
decode(#mysql_query{step = 0, metadata_mark = MetadataMark}, Packet, _Capability) ->
    case mysql_util:parser_integer(Packet) of
        {ColumnCount, <<>>} ->
            {column_count, ColumnCount, MetadataMark};
        {_, _} ->
            {error, unknown_packet_form}
    end;
decode(#mysql_query{step = 1}, Packet, Capability) ->
    ColumnDefinition = case ?CLIENT_PROTOCOL_41 == Capability band ?CLIENT_PROTOCOL_41 of
        true ->
            column_definition_41(Packet, Capability, ?COM_QUERY);
        false ->
            column_definition_320(Packet, Capability, ?COM_QUERY)
    end,
    {column_definition, ColumnDefinition};
decode(#mysql_query{step = 2}, <<251:8, _Bin/binary>>, _Capability) ->
    {row, []};
decode(#mysql_query{step = 2}, Packet, _Capability) ->
    RowInfo = case binary:split(Packet, [<<251:8>>], [global]) of
        [_] ->
            mysql_util:parser_string_lenencs(Packet);
        [H|BinList] ->
            Tail = lists:append([[<<"null">> | mysql_util:parser_string_lenencs(Bin)]
                || Bin <- BinList]),
            lists:append(mysql_util:parser_string_lenencs(H), Tail)
    end,
    {row, RowInfo}.

%% Protocol::ColumnDefinition41
%% lenenc_str     catalog
%% lenenc_str     schema
%% lenenc_str     table
%% lenenc_str     org_table
%% lenenc_str     name
%% lenenc_str     org_name
%% lenenc_int     length of fixed-length fields [0c]
%% 2              character set
%% 4              column length
%% 1              type
%% 2              flags
%% 1              decimals
%% 2              filler [00] [00]
%%   if command was COM_FIELD_LIST {
%% lenenc_int     length of default-values
%% string[$len]   default values
%%   }
column_definition_41(Bin, _Capability, COM) ->
    {Catalog, Rest1} = mysql_util:parser_string_lenenc(Bin),
    {Schema, Rest2} = mysql_util:parser_string_lenenc(Rest1),
    {Table, Rest3} = mysql_util:parser_string_lenenc(Rest2),
    {OrgTable, Rest4} = mysql_util:parser_string_lenenc(Rest3),
    {Name, Rest5} = mysql_util:parser_string_lenenc(Rest4),
    {OrgName, Rest6} = mysql_util:parser_string_lenenc(Rest5),
    {_NextLength, 
    <<
        CharacterSet:16/little, 
        ColumnLength:32/little, 
        Type:8, 
        Flags:16/little, 
        Decimals:8, 
        _Filler:16/little, 
        Rest7/binary
    >>} = mysql_util:parser_integer(Rest6),
    case COM == ?COM_FIELD_LIST of
        true ->
            {DefaultValues, _Rest8} = mysql_util:parser_string_lenenc(Rest7);
        false ->
            DefaultValues = <<>>
    end,
    [Catalog, Schema, Table, OrgTable, Name, OrgName, CharacterSet,
        ColumnLength, Type, Flags, Decimals, DefaultValues].

%% Protocol::ColumnDefinition320
%% lenenc-str     table
%% lenenc-str     name
%% lenenc_int     [03] length of the column_length field
%% 3              column_length
%% lenenc_int     [01] length of type field
%% 1              type
%%   if capabilities & CLIENT_LONG_FLAG {
%% lenenc_int     [03] length of flags+decimals fields
%% 2              flags
%% 1              decimals
%%   } else {
%% 1              [02] length of flags+decimals fields
%% 1              flags
%% 1              decimals
%%   }
%%   if command was COM_FIELD_LIST {
%% lenenc_int     length of default-values
%% string[$len]   default values
%%   }
column_definition_320(Bin, Capability, COM) ->
    {Table, Rest1} = mysql_util:parser_string_lenenc(Bin),
    {Name, Rest2} = mysql_util:parser_string_lenenc(Rest1),
    {_NextLength, <<ColumnLength:24/little, Rest3/binary>>} = mysql_util:parser_integer(Rest2),
    {_LengthOfTypeField, <<Type:8, Rest4/binary>>} = mysql_util:parser_integer(Rest3),
    case ?CLIENT_LONG_FLAG == ?CLIENT_LONG_FLAG band Capability of
        true ->
            {_Len1, <<Flags:16/little, Decimals:8, Rest5/binary>>} = mysql_util:parser_integer(Rest4);
        false ->
            <<_Len1:8, Flags:8, Decimals:8, Rest5/binary>> = Rest4
    end,
    case COM == ?COM_FIELD_LIST of
        true ->
            {DefaultValues, _Rest6} = mysql_util:parser_string_lenenc(Rest5);
        false ->
            DefaultValues = <<>>
    end,
    [Table, Name, ColumnLength, Type, Flags, Decimals, DefaultValues].
