%% @author ningguoqiang <ngq_scut@126.com>

-module(mysql_auth).

-include("mysql.hrl").

-export([
    handshake_response/3,
    handshake_other/1
]).

%% for Old Password Authentication
-define(CODE_N1,    1345345333).
-define(CODE_N2,    305419889).
-define(CODE_SUM,   7).
-define(MASK_30,    1073741823).
-define(MASK_31,    2147483647).

%%%===================================================================
%%% API
%%%===================================================================
handshake_response(#mysql_packet{
    sequence_id = Index,
    payload = #mysql_handshake{
        version = 9,
        connect_id = ConnectID
    }
} = Packet, UserName, Password) ->
    Res = handshake_response_320(Packet#mysql_packet{
        sequence_id = Index + 1
    }, UserName, Password),
    Res#mysql_handshake_response{
        connect_id = ConnectID,
        version = 9
    };
handshake_response(#mysql_packet{
    sequence_id = Index,
    payload = #mysql_handshake{
        version = 10,
        connect_id = ConnectID,
        capability = Capability
    }
} = Packet, UserName, Password) ->
    NewPacket = Packet#mysql_packet{sequence_id = Index + 1},
    Res =
        case ?CLIENT_PROTOCOL_41 /= (Capability band ?CLIENT_PROTOCOL_41) orelse
            ?CLIENT_SECURE_CONNECTION /= (Capability band ?CLIENT_SECURE_CONNECTION) of
            true ->
                handshake_response_320(NewPacket, UserName, Password);
            false ->
                handshake_response_41(NewPacket, UserName, Password)
        end,
    Res#mysql_handshake_response{
        connect_id = ConnectID,
        version = 10
    }.

handshake_other(#mysql_auth_switch{}) ->
    skip;
handshake_other(#mysql_auth_more{}) ->
    skip.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handshake_response_41(#mysql_packet{
    sequence_id = Index,
    payload = #mysql_handshake{
        scramble = Scramble,
        capability = ServerCapability,
        character_set = CharacterSet,
        auth_plugin_name = AuthPlugin
    }
}, UserName, Password) ->
    Capability =
        ?CLIENT_PLUGIN_AUTH bor 
        % ?CLIENT_LONG_PASSWORD bor
        ?CLIENT_LONG_FLAG bor
        ?CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA bor
        ?CLIENT_MULTI_STATEMENTS bor % 支持多语句查询
        % ?CLIENT_SECURE_CONNECTION bor
        ?CLIENT_PROTOCOL_41,
    AuthResponse =
        case ?CLIENT_PLUGIN_AUTH /= (ServerCapability band ?CLIENT_PLUGIN_AUTH) of
            true ->
                auth(<<"mysql_native_password">>, Scramble, Password);
            false ->
                auth(AuthPlugin, Scramble, Password)
        end,
    Payload = list_to_binary([
        <<Capability:32/little, ?MAX_PACKET_SIZE:32/little, CharacterSet:8, 0:23/integer-unit:8>>,
        UserName, <<0:8>>, 
        mysql_util:encode_string_lenenc(AuthResponse),
        AuthPlugin, <<0:8>>
    ]),
    Len = byte_size(Payload),
    #mysql_handshake_response{
        capability = Capability,
        response = <<Len:24/little, Index:8, Payload/binary>>
    }.

handshake_response_320(#mysql_packet{
    sequence_id = Index,
    payload = #mysql_handshake{
        scramble = Scramble
    }
}, UserName, Password) ->
    Capability = 
        ?CLIENT_LONG_PASSWORD bor 
        ?CLIENT_LONG_FLAG bor
        ?CLIENT_TRANSACTIONS,
    AuthResponse = auth(<<"mysql_old_password">>, Scramble, Password),
    Payload = list_to_binary([
        <<Capability:16/little, ?MAX_PACKET_SIZE:24/little>>,
        UserName, <<0:8>>, 
        AuthResponse, <<0:8>>
    ]),
    Len = byte_size(Payload),
    #mysql_handshake_response{
        capability = Capability,
        response = <<Len:24/little, Index:8, Payload/binary>>
    }.

%% [WARNING]Algorithm-MYSQL323 broken!
auth(<<"mysql_old_password">>, Scramble, Password) ->
    {AuthPluginData, _} = erlang:split_binary(Scramble, 8),
    {P1, P2} = hash_old(binary_to_list(Password), ?CODE_N1, ?CODE_N2, ?CODE_SUM),
    {S1, S2} = hash_old(binary_to_list(AuthPluginData), ?CODE_N1, ?CODE_N2, ?CODE_SUM),
    Seed1 = P1 bxor S1,
    Seed2 = P2 bxor S2,
    {Extra, List} = rnd(9, Seed1, Seed2),
    list_to_binary([E bxor Extra || E <- List]);
%% SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
auth(<<"mysql_native_password">>, Scramble, Password) ->
    {AuthPluginData, _} = erlang:split_binary(Scramble, 20),
    Pwd = crypto:hash(sha, Password),
    Pwd1 = crypto:hash(sha, Pwd),
    Context1 = crypto:hash_init(sha),
    Context2 = crypto:hash_update(Context1, AuthPluginData),
    Context3 = crypto:hash_update(Context2, Pwd1),
    Pwd2 = crypto:hash_final(Context3),
    bxor_binary(binary_to_list(Pwd), binary_to_list(Pwd2), []);
%% SHA256( password ) XOR SHA256( "20-bytes random data from server" <concat> SHA256( SHA256( password ) ) )
auth(<<"caching_sha2_password">>, Scramble, Password) ->
    {AuthPluginData, _} = erlang:split_binary(Scramble, 20),
    Pwd = crypto:hash(sha256, Password),
    Pwd1 = crypto:hash(sha256, Pwd),
    Pwd2 = crypto:hash(sha256, <<Pwd1/binary, AuthPluginData/binary>>),
    bxor_binary(binary_to_list(Pwd), binary_to_list(Pwd2), []);
auth(<<>>, Scramble, Password) ->
    auth(?DEFAUL_AUTH, Scramble, Password).

bxor_binary([H1|T1], [H2|T2], Res) ->
    bxor_binary(T1, T2, [H1 bxor H2 | Res]);
bxor_binary([], _, Res) ->
    list_to_binary(lists:reverse(Res));
bxor_binary(_, [], Res) ->
    list_to_binary(lists:reverse(Res)).

hash_old([C | S], N1, N2, Add) ->
    N1_1 = N1 bxor (((N1 band 63) + Add) * C + N1 * 256),
    N2_1 = N2 + ((N2 * 256) bxor N1_1),
    Add_1 = Add + C,
    hash_old(S, N1_1, N2_1, Add_1);
hash_old([], N1, N2, _Add) ->
    {N1 band ?MASK_31 , N2 band ?MASK_31}.

rnd(N, Seed1, Seed2) ->
    do_rnd(N, [], Seed1 rem ?MASK_30, Seed2 rem ?MASK_30).

do_rnd(0, [H|T], _, _) ->
    {H - 64, lists:reverse(T)};
do_rnd(N, List, Seed1, Seed2) ->
    NSeed1 = (Seed1 * 3 + Seed2) rem ?MASK_30,
    NSeed2 = (NSeed1 + Seed2 + 33) rem ?MASK_30,
    Float = (float(NSeed1) / float(?MASK_30)) * 31,
    Val = trunc(Float) + 64,
    do_rnd(N - 1, [Val | List], NSeed1, NSeed2).
