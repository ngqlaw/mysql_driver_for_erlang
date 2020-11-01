%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_util).

-include("mysql.hrl").

-export([
	parser_integer/1,
	parser_string_null/1,
	parser_string_lenenc/1,
	parser_string_lenencs/1,
	encode_string_lenenc/1, encode_string_lenenc/2
]).

parser_integer(<<252:8, Integer:16/little, Rest/binary>>) ->
	{Integer, Rest};
parser_integer(<<253:8, Integer:24/little, Rest/binary>>) ->
	{Integer, Rest};
parser_integer(<<254:8, Integer:64/little, Rest/binary>>) ->
	{Integer, Rest};
parser_integer(<<254:8, Rest/binary>>) when byte_size(Rest) < 8 ->
	eof;
parser_integer(<<Integer:8, Rest/binary>>) ->
	{Integer, Rest}.

parser_string_null(Bin) ->
	parser_string_null(Bin, <<>>).
parser_string_null(<<I:8,Rest/binary>>, Result) when I > 0 ->
	parser_string_null(Rest, <<Result/binary, I:8>>);
parser_string_null(<<_:8, Bin/binary>>, Result) ->
	{Result, Bin}.

parser_string_lenenc(Bin) ->
	{Len, Rest} = parser_integer(Bin),
	erlang:split_binary(Rest, Len).

parser_string_lenencs(Bin) ->
	parser_string_lenencs(Bin, []).

parser_string_lenencs(<<>>, Acc) ->
	lists:reverse(Acc);
parser_string_lenencs(Bin, Acc) ->
	{Value, Rest} = parser_string_lenenc(Bin),
	parser_string_lenencs(Rest, [Value|Acc]).

encode_string_lenenc(Bin) ->
	Len = byte_size(Bin),
	encode_string_lenenc(Len, Bin).
encode_string_lenenc(Len, Bin) when Len < 252 ->
	<<Len:8, Bin/binary>>;
encode_string_lenenc(Len, Bin) ->
	<<252:8, Len:16/little, Bin/binary>>.
