-ifndef(MYSQL_H).
-define(MYSQL_H, true).

-ifdef(OTP_RELEASE). %% this implies 21 or higher
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-else.
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-define(GET_STACK(_), erlang:get_stacktrace()).
-endif.

-define(OPTIONS, [
	  binary,
	  {packet, 0},
	  {reuseaddr, true},
	  {nodelay, false},
	  {send_timeout, 5000},
	  {exit_on_close, true},
      {active, false}
	 ]).

-define(MAX_PACKET_SIZE, 1048576). % setting accepted single packet size limit
-define(MAX_LIMIT_SIZE, 16777216). % mysql single packet size limit
-define(DEFAUL_AUTH, <<"caching_sha2_password">>).

%% Capability Flags
-define(CLIENT_LONG_PASSWORD, 					16#00000001).
-define(CLIENT_FOUND_ROWS, 						16#00000002).
-define(CLIENT_LONG_FLAG, 						16#00000004).
-define(CLIENT_CONNECT_WITH_DB, 				16#00000008).
-define(CLIENT_NO_SCHEMA, 						16#00000010).
-define(CLIENT_COMPRESS, 						16#00000020).
-define(CLIENT_ODBC, 							16#00000040).
-define(CLIENT_LOCAL_FILES, 					16#00000080).
-define(CLIENT_IGNORE_SPACE,					16#00000100).
-define(CLIENT_PROTOCOL_41, 					16#00000200).
-define(CLIENT_INTERACTIVE, 					16#00000400).
-define(CLIENT_SSL, 							16#00000800).
-define(CLIENT_IGNORE_SIGPIPE, 					16#00001000).
-define(CLIENT_TRANSACTIONS, 					16#00002000).
-define(CLIENT_RESERVED, 						16#00004000).
-define(CLIENT_SECURE_CONNECTION, 				16#00008000).
-define(CLIENT_MULTI_STATEMENTS, 				16#00010000).
-define(CLIENT_MULTI_RESULTS, 					16#00020000).
-define(CLIENT_PS_MULTI_RESULTS, 				16#00040000).
-define(CLIENT_PLUGIN_AUTH, 					16#00080000).
-define(CLIENT_CONNECT_ATTRS, 					16#00100000).
-define(CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA, 	16#00200000).
-define(CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS, 	16#00400000).
-define(CLIENT_SESSION_TRACK, 					16#00800000).
-define(CLIENT_DEPRECATE_EOF, 					16#01000000).
-define(CLIENT_OPTIONAL_RESULTSET_METADATA,     16#02000000).

%% Status Flags
-define(SERVER_STATUS_IN_TRANS, 				16#0001).
-define(SERVER_STATUS_AUTOCOMMIT, 				16#0002).
-define(SERVER_MORE_RESULTS_EXISTS, 			16#0008).
-define(SERVER_STATUS_NO_GOOD_INDEX_USED, 		16#0010).
-define(SERVER_STATUS_NO_INDEX_USED, 			16#0020).
-define(SERVER_STATUS_CURSOR_EXISTS, 			16#0040).
-define(SERVER_STATUS_LAST_ROW_SENT, 			16#0080).
-define(SERVER_STATUS_DB_DROPPED, 				16#0100).
-define(SERVER_STATUS_NO_BACKSLASH_ESCAPES, 	16#0200).
-define(SERVER_STATUS_METADATA_CHANGED, 		16#0400).
-define(SERVER_QUERY_WAS_SLOW, 					16#0800).
-define(SERVER_PS_OUT_PARAMS, 					16#1000).
-define(SERVER_STATUS_IN_TRANS_READONLY, 		16#2000).
-define(SERVER_SESSION_STATE_CHANGED, 			16#4000).

%% types of state change information
-define(SESSION_TRACK_SYSTEM_VARIABLES, 		0).
-define(SESSION_TRACK_SCHEMA, 					1).
-define(SESSION_TRACK_STATE_CHANGE, 			2).

%% Command
-define(COM_TEXT,                       -1).
-define(COM_SLEEP, 						0).
-define(COM_QUIT, 						1).
-define(COM_INIT_DB, 					2).
-define(COM_QUERY, 						3).
-define(COM_FIELD_LIST, 				4).
-define(COM_CREATE_DB, 					5).
-define(COM_DROP_DB, 					6).
-define(COM_REFRESH, 					7).
-define(COM_SHUTDOWN, 					8).
-define(COM_STATISTICS, 				9).
-define(COM_PROCESS_INFO, 				10).
-define(COM_CONNECT, 					11).
-define(COM_PROCESS_KILL, 				12).
-define(COM_DEBUG, 						13).
-define(COM_PING, 						14).
-define(COM_TIME, 						15).
-define(COM_DELAYED_INSERT, 			16).
-define(COM_CHANGE_USER, 				17).
-define(COM_BINLOG_DUMP, 				18).
-define(COM_TABLE_DUMP, 				19).
-define(COM_CONNECT_OUT, 				20).
-define(COM_REGISTER_SLAVE, 			21).
-define(COM_STMT_PREPARE, 				22).
-define(COM_STMT_EXECUTE, 				23).
-define(COM_STMT_SEND_LONG_DATA, 		24).
-define(COM_STMT_CLOSE, 				25).
-define(COM_STMT_RESET, 				26).
-define(COM_SET_OPTION, 				27).
-define(COM_STMT_FETCH, 				28).
-define(COM_DAEMON, 					29).
-define(COM_BINLOG_DUMP_GTID, 			30).
-define(COM_RESET_CONNECTION, 			31).

%% Column flag
-define(NOT_NULL_FLAG,                  1).     % Field can't be NULL. More...
-define(PRI_KEY_FLAG,                   2).     % Field is part of a primary key. More...
-define(UNIQUE_KEY_FLAG,                4).     % Field is part of a unique key. More...
-define(MULTIPLE_KEY_FLAG,              8).     % Field is part of a key. More...
-define(BLOB_FLAG,                      16).    % Field is a blob. More...
-define(UNSIGNED_FLAG,                  32).    % Field is unsigned. More...
-define(ZEROFILL_FLAG,                  64).    % Field is zerofill. More...
-define(BINARY_FLAG,                    128).   % Field is binary. More...
-define(ENUM_FLAG,                      256).   % field is an enum More...
-define(AUTO_INCREMENT_FLAG,            512).   % field is a autoincrement field More...
-define(TIMESTAMP_FLAG,                 1024).  % Field is a timestamp. More...
-define(SET_FLAG,                       2048).  % field is a set More...
-define(NO_DEFAULT_VALUE_FLAG,          4096).  % Field doesn't have default value. More...
-define(ON_UPDATE_NOW_FLAG,             8192).  % Field is set to NOW on UPDATE. More...
-define(NUM_FLAG,                       32768). % Field is num (for clients) More...
-define(PART_KEY_FLAG,                  16384). % Intern; Part of some key. More...
-define(GROUP_FLAG,                     32768). % Intern: Group field. More...
-define(UNIQUE_FLAG,                    65536). % Intern: Used by sql_yacc. More...
-define(BINCMP_FLAG,                    131072).% Intern: Used by sql_yacc. More...
-define(GET_FIXED_FIELDS_FLAG,          262144).% Used to get fields in item tree . More...
-define(FIELD_IN_PART_FUNC_FLAG,        524288).% Field part of partition func. More...
-define(FIELD_IN_ADD_INDEX,             1048576).   % Intern: Field in TABLE object for new version of altered table, which participates in a newly added index. More...
-define(FIELD_IS_RENAMED,               2097152).   % Intern: Field is being renamed. More...
-define(FIELD_FLAGS_STORAGE_MEDIA,      22).        % Field storage media, bit 22-23. More...
-define(FIELD_FLAGS_STORAGE_MEDIA_MASK, 12582912).
-define(FIELD_FLAGS_COLUMN_FORMAT,      24).        % Field column format, bit 24-25. More...
-define(FIELD_FLAGS_COLUMN_FORMAT_MASK, 50331648).
-define(FIELD_IS_DROPPED,               67108864).  % Intern: Field is being dropped. More...
-define(EXPLICIT_NULL_FLAG,             134217728). % Field is explicitly specified as \ NULL by the user. More...
-define(FIELD_IS_MARKED,                268435456). % Intern: field is marked, \ general purpose. More...
-define(NOT_SECONDARY_FLAG,             536870912). % Field will not be loaded in secondary engine. More...

%% Column Types
-define(MYSQL_TYPE_DECIMAL,         0).
-define(MYSQL_TYPE_TINY,            1).
-define(MYSQL_TYPE_SHORT,           2).
-define(MYSQL_TYPE_LONG,            3).
-define(MYSQL_TYPE_FLOAT,           4).
-define(MYSQL_TYPE_DOUBLE,          5).
-define(MYSQL_TYPE_NULL,            6).
-define(MYSQL_TYPE_TIMESTAMP,       7).
-define(MYSQL_TYPE_LONGLONG,        8).
-define(MYSQL_TYPE_INT24,           9).
-define(MYSQL_TYPE_DATE,            10).
-define(MYSQL_TYPE_TIME,            11).
-define(MYSQL_TYPE_DATETIME,        12).
-define(MYSQL_TYPE_YEAR,            13).
-define(MYSQL_TYPE_NEWDATE,         14).
-define(MYSQL_TYPE_VARCHAR,         15).
-define(MYSQL_TYPE_BIT,             16).
-define(MYSQL_TYPE_TIMESTAMP2,      17).
-define(MYSQL_TYPE_DATETIME2,       18).
-define(MYSQL_TYPE_TIME2,           19).
-define(MYSQL_TYPE_TYPED_ARRAY,     20).
-define(MYSQL_TYPE_INVALID,         243).
-define(MYSQL_TYPE_BOOL,            244).
-define(MYSQL_TYPE_JSON,            245).
-define(MYSQL_TYPE_NEWDECIMAL,      246).
-define(MYSQL_TYPE_ENUM,            247).
-define(MYSQL_TYPE_SET,             248).
-define(MYSQL_TYPE_TINY_BLOB,       249).
-define(MYSQL_TYPE_MEDIUM_BLOB,     250).
-define(MYSQL_TYPE_LONG_BLOB,       251).
-define(MYSQL_TYPE_BLOB,            252).
-define(MYSQL_TYPE_VAR_STRING,      253).
-define(MYSQL_TYPE_STRING,          254).
-define(MYSQL_TYPE_GEOMETRY,        255).


-define(RESULTSET_METADATA_NONE, 0).
-define(RESULTSET_METADATA_FULL, 1).

-define(MYSQL_FLAG_QUERY, query_sql).
-define(MYSQL_FLAG_HANDSHAKE, handshake).

-record(mysql_handshake, {
    version = 0,
    version_desc = <<>>,
    connect_id = 0,
    scramble = <<>>,
    capability = 0,
    status_flags = 0,
    character_set = 0,
    auth_plugin_name = <<>>
}).

-record(mysql_auth_switch, {
    plugin_name = <<>>,
    data = <<>>
}).

-record(mysql_auth_more ,{
    data = <<>>
}).

-record(mysql_handshake_response, {
    connect_id = 0,
    version = 0,
    capability = 0,
    response = <<>>
}).

-record(mysql_packet, {
    sequence_id = 0,
    payload,
    buff = <<>>
}).

-record(mysql_ok_packet, {
    affected_rows = 0,
    last_insert_id = 0,
    status_flags = 0,
    warnings = 0,
    session_state_info = [],
    info = <<>>
}).

-record(mysql_err_packet, {
    code = 0,
    sql_state_marker = <<>>,
    sql_state = <<>>,
    message = <<>>
}).

-record(mysql_eof_packet, {
    warnings = 0,
    status_flags = 0
}).

-record(mysql_local_infile, {
    filename = <<>>
}).

-record(mysql_query, {
    step = 0,
    is_eof = true,
    metadata_mark = 0,
    column = 0,
    columns = [],
    rows = []
}).

-record(mysql_result, {
    is_reply = false,               % false(not reply) | true(reply)
    reply = [],
    result = undefined
}).

-record(mysql_handle, {
    msg = <<>>,                     % common string
    from,                           % request pid
    flag = query_sql,               % for now only support:query_sql
    packet, 
    result
}).

-endif.