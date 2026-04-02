// =============================================================================
//  postgres.inc  —  PostgreSQL Plugin for SA-MP / open.mp
//  Version: 1.0.0
//
//  Features
//  --------
//    1. Connection management   (sync + async)
//    2. Query execution         (sync + async with callback format-string)
//    3. Prepared statements     (pg_prepare / pg_exec_prepared / pg_exec_prepared_sync)
//    4. Cache / result API      (pg_cache_get_value, pg_cache_get_value_index, ...)
//    5. Field metadata          (pg_field_name, pg_field_index)
//    6. Transaction API         (pg_begin/commit/rollback, async variants)
//    7. Advanced escaping       (pg_escape_literal, pg_escape_identifier)
//    8. Pipeline mode           (pg_enter_pipeline_mode, pg_exit_pipeline_mode)
//    9. Built-in keepalive      (automatic, runs every 60 s internally)
// =============================================================================

#if defined _postgres_included
    #endinput
#endif
#define _postgres_included

// =============================================================================
//  Constants
// =============================================================================

/// Returned by pg_connect / pg_connect_async callback on failure
#define PG_INVALID_HANDLE   PG:0

/// Returned by pg_query_sync / pg_exec_prepared_sync on failure
#define PG_INVALID_RESULT   PGResult:0

// =============================================================================
//  CONNECTION
// =============================================================================

/**
 * pg_connect(host[], user[], password[], database[], port = 5432)
 *
 * Opens a synchronous connection to a PostgreSQL server.
 *
 * @param  host        Server hostname or IP.
 * @param  user        PostgreSQL username.
 * @param  password    Password.
 * @param  database    Target database.
 * @param  port        Server port (default 5432).
 * @return             Connection handle > 0, or PG_INVALID_HANDLE on failure.
 *
 * Example:
 *   new pg = pg_connect("127.0.0.1", "root", "secret", "game");
 *   if (pg == PG_INVALID_HANDLE) print("Connection failed");
 */
native PG:pg_connect(const host[], const user[], const password[], const database[], port = 5432);

/**
 * pg_close(connection_handle)
 *
 * Closes a connection and frees its resources.
 *
 * @param  connection_handle  Handle returned by pg_connect.
 */
native pg_close(PG:connection_handle);

/**
 * pg_connect_async(host[], user[], password[], database[], port, callback[])
 *
 * Opens a connection on a background thread and calls the Pawn public when done.
 * The connection is opened using the same logic as pg_connect, but without
 * blocking the server's main thread.
 *
 * Callback signature:
 *   forward OnPgConnected(connection_handle);
 *   public  OnPgConnected(connection_handle) {
 *       if (connection_handle == PG_INVALID_HANDLE) { // error }
 *   }
 *
 * @param  host, user, password, database, port   Connection parameters.
 * @param  callback   Name of the Pawn public to invoke when the connection
 *                    attempt finishes. Receives connection_handle as its
 *                    sole argument (0 = failure).
 */
native pg_connect_async(const host[], const user[], const password[],
                        const database[], port, const callback[]);

// =============================================================================
//  QUERY — ASYNC (with optional callback parameters)
// =============================================================================

/**
 * pg_query(connection_handle, query[], callback[], format[]="", {Float,_}:...)
 *
 * Executes a query asynchronously. The named Pawn public is invoked on the
 * next server tick after the query finishes. Extra parameters given after
 * the format string are forwarded verbatim to the callback.
 *
 * Format specifiers:
 *   'd' or 'i'  — integer (cell)
 *   'f'         — float
 *   's'         — string (packed to IPC memory before dispatch)
 *
 * Callback signature with extra params:
 *   forward OnResult(result_handle, playerid, Float:distance);
 *   public  OnResult(result_handle, playerid, Float:distance) { ... }
 *
 * @param  connection_handle  Handle from pg_connect.
 * @param  query              SQL string.
 * @param  callback           Name of the Pawn public.
 * @param  format             Optional format string (default empty).
 * @param  ...                Extra values forwarded to the callback.
 * @return 1 if enqueued, 0 on error.
 *
 * Examples:
 *   // Basic usage (no extra params):
 *   pg_query(pg, "SELECT * FROM players", "OnPlayersLoaded");
 *
 *   // With extra params:
 *   pg_query(pg, "SELECT * FROM players WHERE id = 1",
 *            "OnPlayerLoaded", "d", playerid);
 *
 *   // Multiple params:
 *   pg_query(pg, "SELECT 1", "OnDone", "dfs", playerid, 3.14, player_name);
 */
native pg_query(PG:connection_handle, const query[], const callback[],
                const format[] = "", {Float, _}: ...);

/**
 * pg_query_sync(connection_handle, query[])
 *
 * Executes a query synchronously. Blocks the current tick.
 * Prefer pg_query for queries that may take time.
 *
 * @return Result handle > 0, or PG_INVALID_RESULT on failure.
 *
 * Example:
 *   new res = pg_query_sync(pg, "SELECT COUNT(*) FROM players");
 *   printf("rows: %d", pg_num_rows(res));
 *   pg_free_result(res);
 */
native PGResult:pg_query_sync(PG:connection_handle, const query[]);

// =============================================================================
//  RESULT SET — BASIC
// =============================================================================

/**
 * pg_num_rows(result_handle)   -> int
 * Returns the number of rows in a result set.
 */
native pg_num_rows(PGResult:result_handle);

/**
 * pg_num_fields(result_handle)   -> int
 * Returns the number of columns in a result set.
 */
native pg_num_fields(PGResult:result_handle);

/**
 * pg_get_field(result_handle, row, column_index, dest[], size = sizeof dest)
 *
 * Retrieves a field value by zero-based column index.
 *
 * @param  result_handle  Result from pg_query_sync or async callback.
 * @param  row            Zero-based row index.
 * @param  column_index   Zero-based column index.
 * @param  dest           Output buffer.
 * @param  size           Buffer size.
 * @return 1 on success, 0 on out-of-range or invalid handle.
 */
native pg_get_field(PGResult:result_handle, row, column_index, dest[], size = sizeof dest);

/**
 * pg_free_result(result_handle)
 * Frees a result set. Call this after you are done reading the result.
 */
native pg_free_result(PGResult:result_handle);

// =============================================================================
//  RESULT SET — CACHE API
// =============================================================================

/**
 * pg_cache_get_value(result, row, column_name[], dest[], size = sizeof dest)
 *
 * Retrieves a field value by column name.
 *
 * @param  column_name  Name of the column (case-sensitive).
 * @return 1 on success, 0 if column not found or row out of range.
 *
 * Example:
 *   new name[64];
 *   pg_cache_get_value(res, 0, "player_name", name);
 */
native pg_cache_get_value(PGResult:result, row, const column_name[], dest[],
                          size = sizeof dest);

/**
 * pg_cache_get_value_index(result, row, column_index, dest[], size = sizeof dest)
 *
 * Retrieves a field value by zero-based column index.
 * Equivalent to pg_get_field.
 */
native pg_cache_get_value_index(PGResult:result, row, column_index, dest[],
                                size = sizeof dest);

/**
 * pg_cache_get_row_count(result)   -> int
 * Returns the number of rows. Equivalent to pg_num_rows.
 */
native pg_cache_get_row_count(PGResult:result);

/**
 * pg_cache_get_field_count(result)   -> int
 * Returns the number of columns. Equivalent to pg_num_fields.
 */
native pg_cache_get_field_count(PGResult:result);

// =============================================================================
//  FIELD METADATA
// =============================================================================

/**
 * pg_field_name(result, column_index, dest[], size = sizeof dest)
 *
 * Copies the name of the column at column_index into dest[].
 *
 * Example:
 *   new col_name[64];
 *   pg_field_name(res, 0, col_name);
 *   printf("column 0: %s", col_name);
 */
native pg_field_name(PGResult:result, column_index, dest[], size = sizeof dest);

/**
 * pg_field_index(result, column_name[])
 *
 * Returns the zero-based index of a column by name, or -1 if not found.
 *
 * Example:
 *   new idx = pg_field_index(res, "player_name");
 */
native pg_field_index(PGResult:result, const column_name[]);

// =============================================================================
//  PREPARED STATEMENTS
// =============================================================================

/**
 * pg_prepare(connection_handle, name[], query[])
 *
 * Sends a named prepared statement to the server (PQprepare).
 * The statement is stored server-side and can then be executed cheaply
 * with pg_exec_prepared / pg_exec_prepared_sync.
 *
 * @param  name   Unique statement name (must be unique per connection).
 * @param  query  Parameterless SQL string.
 * @return 1 on success, 0 on failure.
 *
 * Example:
 *   pg_prepare(pg, "sel_player", "SELECT name, score FROM players WHERE id = 1");
 */
native pg_prepare(PG:connection_handle, const name[], const query[]);

/**
 * pg_exec_prepared(connection_handle, name[], callback[], format[]="", {Float,_}:...)
 *
 * Asynchronously executes a previously prepared statement and invokes
 * the Pawn callback with the result handle (plus any extra params).
 *
 * @return 1 if enqueued, 0 on invalid handle.
 *
 * Example:
 *   pg_exec_prepared(pg, "sel_player", "OnPlayerDataLoaded", "d", playerid);
 */
native pg_exec_prepared(PG:connection_handle, const name[], const callback[],
                        const format[] = "", {Float, _}: ...);

/**
 * pg_exec_prepared_sync(connection_handle, name[])
 *
 * Synchronously executes a previously prepared statement.
 *
 * @return Result handle > 0, or PG_INVALID_RESULT on failure.
 */
native PGResult:pg_exec_prepared_sync(PG:connection_handle, const name[]);

// =============================================================================
//  TRANSACTION API
// =============================================================================

/**
 * pg_begin(connection_handle)    — synchronous BEGIN
 * pg_commit(connection_handle)   — synchronous COMMIT
 * pg_rollback(connection_handle) — synchronous ROLLBACK
 *
 * @return 1 on success (PGRES_COMMAND_OK), 0 on failure.
 *
 * Sync example:
 *   pg_begin(pg);
 *   pg_query_sync(pg, "UPDATE accounts SET balance = balance - 100 WHERE id = 1");
 *   pg_query_sync(pg, "UPDATE accounts SET balance = balance + 100 WHERE id = 2");
 *   pg_commit(pg);
 */
native pg_begin(PG:connection_handle);
native pg_commit(PG:connection_handle);
native pg_rollback(PG:connection_handle);

/**
 * pg_begin_async(connection_handle, callback[]="")
 * pg_commit_async(connection_handle, callback[]="")
 * pg_rollback_async(connection_handle, callback[]="")
 *
 * Async variants. The optional callback is invoked with result_handle = 1
 * on success, 0 on failure.
 *
 * @return 1 if enqueued, 0 on invalid handle.
 *
 * Async example:
 *   pg_begin_async(pg, "OnTxStarted");
 *   forward OnTxStarted(result_handle);
 *   public  OnTxStarted(result_handle) {
 *       if (!result_handle) { print("BEGIN failed"); return; }
 *       pg_query(pg, "UPDATE ...", "OnUpdateDone");
 *   }
 */
native pg_begin_async(PG:connection_handle, const callback[] = "");
native pg_commit_async(PG:connection_handle, const callback[] = "");
native pg_rollback_async(PG:connection_handle, const callback[] = "");

// =============================================================================
//  ESCAPE FUNCTIONS
// =============================================================================

/**
 * pg_escape_string(connection_handle, input[], output[], size = sizeof output)
 *
 * Basic string escaping (PQescapeStringConn). Does NOT add surrounding quotes.
 * Use pg_escape_literal for safe interpolation inside SQL strings.
 */
native pg_escape_string(PG:connection_handle, const input[], output[],
                        size = sizeof output);

/**
 * pg_escape_literal(connection_handle, input[], output[], size = sizeof output)
 *
 * Escapes input and wraps it in single quotes ready for SQL interpolation
 * (PQescapeLiteral). The output includes the surrounding single quotes.
 *
 * Example:
 *   new safe[256];
 *   pg_escape_literal(pg, player_name, safe);
 *   // safe is now: 'O''Brien'
 *   format(query, sizeof query, "SELECT * FROM players WHERE name = %s", safe);
 */
native pg_escape_literal(PG:connection_handle, const input[], output[],
                         size = sizeof output);

/**
 * pg_escape_identifier(connection_handle, input[], output[], size = sizeof output)
 *
 * Escapes an identifier (table/column name) and wraps it in double quotes
 * (PQescapeIdentifier). Prevents SQL injection via dynamic identifier names.
 *
 * Example:
 *   new safe_table[128];
 *   pg_escape_identifier(pg, table_name, safe_table);
 *   // safe_table is now: "players"
 */
native pg_escape_identifier(PG:connection_handle, const input[], output[],
                             size = sizeof output);

// =============================================================================
//  FORMAT HELPER
// =============================================================================

/**
 * pg_format(connection_handle, output[], size = sizeof output, const format[], {Float,_}:...)
 *
 * Formats a SQL query string in-place, similar to mysql_format from SA-MP-MySQL.
 * Unlike Pawn's built-in format(), pg_format adds the %e and %E specifiers
 * that automatically escape values using the PostgreSQL connection, preventing
 * SQL injection without the need for separate pg_escape_literal calls.
 *
 * Format specifiers:
 *   %d / %i  — integer (cell)
 *   %f       — float
 *   %s       — raw string (no escaping — use only with trusted input)
 *   %e       — escaped literal  (PQescapeLiteral, wraps in single quotes)
 *   %E       — escaped identifier (PQescapeIdentifier, wraps in double quotes)
 *   %%       — literal '%'
 *
 * @param  connection_handle  Handle returned by pg_connect (required for escaping).
 * @param  output             Destination buffer.
 * @param  size               Buffer size (default = sizeof output).
 * @param  format             Format string.
 * @param  ...                Values to substitute.
 * @return Number of characters written (excluding null terminator), or -1 on error.
 *
 * Example — safe INSERT without manual escape steps:
 *   new query[512];
 *   pg_format(g_PG, query, sizeof query,
 *       "INSERT INTO logs.chat_log (m_nick, m_idc, m_m, m_time) VALUES (%e, %d, %e, TO_TIMESTAMP(%d))",
 *       P_Nome[playerid],
 *       PlayerInfo[playerid][IDConta],
 *       text,
 *       gettime()
 *   );
 *   pg_query(g_PG, query, "");
 *
 * Example — dynamic table name via %E:
 *   new query[256];
 *   pg_format(g_PG, query, sizeof query,
 *       "SELECT * FROM %E WHERE id = %d", table_name, player_id);
 */
native pg_format(PG:connection_handle, output[], size = sizeof output,
                 const format[], {Float, _}: ...);

// =============================================================================
//  PIPELINE MODE  (libpq 14+)
// =============================================================================

/**
 * pg_enter_pipeline_mode(connection_handle)
 *
 * Puts the libpq connection into pipeline mode (PQenterPipelineMode).
 * In pipeline mode multiple queries can be sent without waiting for each
 * result, reducing round-trip latency on high-throughput workloads.
 *
 * WARNING: Only use this if you understand libpq pipeline semantics.
 * The async query dispatch in this plugin (PQsendQuery) is compatible
 * with pipeline mode.
 *
 * @return 1 on success, 0 on failure.
 */
native pg_enter_pipeline_mode(PG:connection_handle);

/**
 * pg_exit_pipeline_mode(connection_handle)
 *
 * Exits pipeline mode (PQexitPipelineMode).
 * All pending pipeline results must have been consumed before calling this.
 *
 * @return 1 on success, 0 on failure.
 */
native pg_exit_pipeline_mode(PG:connection_handle);

// =============================================================================
//  STANDARD CALLBACKS
// =============================================================================

/**
 * Standard async-query callback forwarded here for documentation purposes.
 * The actual name is whatever string you pass to pg_query / pg_exec_prepared.
 *
 *   forward OnQueryExecuted(result_handle);
 *   public  OnQueryExecuted(result_handle) {
 *       for (new r, rows = pg_cache_get_row_count(result_handle); r < rows; ++r)
 *       {
 *           new name[64];
 *           pg_cache_get_value(result_handle, r, "name", name);
 *       }
 *       pg_free_result(result_handle);
 *   }
 */
forward OnQueryExecuted(PGResult:result_handle);

/**
 * Async-connect callback.
 *
 *   forward OnPgConnected(connection_handle);
 *   public  OnPgConnected(connection_handle) {
 *       if (!connection_handle) { print("[ERROR] Connection failed"); return; }
 *       g_pg = connection_handle;
 *   }
 */
forward OnPgConnected(PG:connection_handle);

// =============================================================================
//  CONVENIENCE MACROS
// =============================================================================

/**
 * PG_CALLBACK(%1)
 * Shorthand to declare a named query callback.
 *
 * Usage:
 *   PG_CALLBACK(OnPlayersLoaded) {
 *       // result_handle available here
 *       pg_free_result(result_handle);
 *   }
 */
#define PG_CALLBACK(%1) \
    forward %1(PGResult:result_handle); \
    public  %1(PGResult:result_handle)

/**
 * PG_CALLBACK_EX(%1,%2)
 * Shorthand for a callback with extra parameters.
 *
 * Usage:
 *   PG_CALLBACK_EX(OnPlayerLoaded, playerid) {
 *       new name[64];
 *       pg_cache_get_value(result_handle, 0, "name", name);
 *       pg_free_result(result_handle);
 *   }
 */
#define PG_CALLBACK_EX(%1,%2) \
    forward %1(PGResult:result_handle, %2); \
    public  %1(PGResult:result_handle, %2)
