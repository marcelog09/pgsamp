/*
 * natives.cpp
 * Defines all Plugin_Native functions exposed to Pawn scripts.
 *
 * Natives registered:
 *   pg_connect(host[], user[], password[], database[], port)                     -> int handle
 *   pg_close(connection_handle)                                                   -> void
 *   pg_query(connection_handle, query[], callback[], format[]="", {Float,_}:...) -> void
 *   pg_query_sync(connection_handle, query[])                                     -> int result_handle
 *   pg_num_rows(result_handle)                                                    -> int
 *   pg_num_fields(result_handle)                                                  -> int
 *   pg_get_field(result_handle, row, column, dest[], size)                        -> bool
 *   pg_escape_string(connection_handle, input[], output[], size)                  -> bool
 *   pg_free_result(result_handle)                                                 -> void
 *
 *   pg_prepare(connection_handle, name[], query[])                                -> bool
 *   pg_exec_prepared(connection_handle, name[], callback[], format[]="", ...)    -> void
 *   pg_exec_prepared_sync(connection_handle, name[])                             -> int result_handle
 *
 *   pg_cache_get_value(result, row, column_name[], dest[], size)                 -> bool
 *   pg_cache_get_value_index(result, row, column_index, dest[], size)            -> bool
 *   pg_cache_get_row_count(result)                                               -> int
 *   pg_cache_get_field_count(result)                                             -> int
 *
 *   pg_field_name(result, column_index, dest[], size)                            -> bool
 *   pg_field_index(result, column_name[])                                        -> int
 *
 *   pg_begin(connection_handle)                                                  -> bool
 *   pg_commit(connection_handle)                                                 -> bool
 *   pg_rollback(connection_handle)                                               -> bool
 *   pg_begin_async(connection_handle, callback[]="")                             -> bool
 *   pg_commit_async(connection_handle, callback[]="")                            -> bool
 *   pg_rollback_async(connection_handle, callback[]="")                          -> bool
 *
 *   pg_escape_literal(connection_handle, input[], output[], size)                -> bool
 *   pg_escape_identifier(connection_handle, input[], output[], size)             -> bool
 *
 *   pg_connect_async(host[], user[], password[], database[], port, callback[])   -> void
 *
 *   pg_enter_pipeline_mode(connection_handle)                                    -> bool
 *   pg_exit_pipeline_mode(connection_handle)                                     -> bool
 *
 *   pg_format(connection_handle, output[], size, format[], {Float,_}:...)        -> int
 */

#define SAMP_SDK_WANT_AMX_EVENTS
#include "../libs/samp-sdk/sdk/samp_sdk.hpp"

#include "connection_manager.h"
#include "result.h"
#include "thread_pool.h"
#include "postgres.h"

#include <libpq-fe.h>
#include <string>
#include <cstring>
#include <atomic>

using namespace PgPlugin;
// Import only what we need from Samp_SDK to avoid ambiguity with PgPlugin::Log
using Samp_SDK::Native_Params;
namespace amx = Samp_SDK::amx;

// ============================================================
// Global flag: when true, string arguments are re-encoded from
// Latin-1 (ISO-8859-1) to UTF-8 before being sent to PostgreSQL.
// Disabled by default; enable via pg_set_charset_latin1(1) in
// OnGameModeInit when the server uses Latin-1 encoded strings
// (typical on Linux SA-MP / open.mp servers).
static std::atomic<bool> g_latin1_convert{false};

// ============================================================
// Returns true if 'in' is valid UTF-8 (including pure ASCII).
// Used to avoid double-encoding strings that are already UTF-8.
static bool is_valid_utf8(const std::string &in)
{
    const unsigned char *p = reinterpret_cast<const unsigned char *>(in.data());
    const unsigned char *end = p + in.size();
    while (p < end)
    {
        unsigned char c = *p;
        if (c < 0x80)
        {
            ++p;
            continue;
        } // ASCII — always valid

        int extra;
        if ((c & 0xE0) == 0xC0)
            extra = 1; // 2-byte sequence
        else if ((c & 0xF0) == 0xE0)
            extra = 2; // 3-byte sequence
        else if ((c & 0xF8) == 0xF0)
            extra = 3; // 4-byte sequence
        else
            return false; // invalid lead byte

        ++p;
        for (int i = 0; i < extra; ++i, ++p)
        {
            if (p >= end || (*p & 0xC0) != 0x80)
                return false; // missing or invalid continuation byte
        }
    }
    return true;
}

// ============================================================
// Re-encodes 'in' from Latin-1 to UTF-8 only when ALL conditions
// are met:
//   1. g_latin1_convert is enabled.
//   2. The PostgreSQL session's client_encoding is "UTF8".
//      If it is LATIN1, WIN1252, etc., the server already
//      handles transcoding internally — converting here too
//      would cause double-encoding (e.g. "vocÃª" instead of
//      "você").
//   3. The string is NOT already valid UTF-8.
// Strings that are pure ASCII or already UTF-8 are returned
// unchanged, avoiding any unnecessary allocation.
static std::string maybe_latin1_to_utf8(const std::string &in, PGconn *conn)
{
    if (!g_latin1_convert.load(std::memory_order_relaxed))
        return in;

    // Only convert when the server session expects UTF-8 bytes.
    // On Windows the default client_encoding is often WIN1252 or
    // LATIN1, meaning libpq/the server transcodes automatically.
    if (conn)
    {
        const char *enc = PQparameterStatus(conn, "client_encoding");
        if (!enc || strcmp(enc, "UTF8") != 0)
            return in;
    }

    if (is_valid_utf8(in))
        return in; // already ASCII or valid UTF-8 — nothing to do

    std::string out;
    out.reserve(in.size() * 2);
    for (unsigned char c : in)
    {
        if (c < 0x80)
            out += static_cast<char>(c);
        else
        {
            out += static_cast<char>(0xC0 | (c >> 6));
            out += static_cast<char>(0x80 | (c & 0x3F));
        }
    }
    return out;
}

// ============================================================
// pg_connect(host[], user[], password[], database[], port)
// Returns connection handle (0 = failure)
// ============================================================
Plugin_Native(pg_connect, AMX *amx, cell *params)
{
    // 5 params
    if (params[0] / sizeof(cell) < 5)
        return 0;

    Native_Params p(amx, params);
    std::string host = p.Get<std::string>(0);
    std::string user = p.Get<std::string>(1);
    std::string password = p.Get<std::string>(2);
    std::string database = p.Get<std::string>(3);
    int port = p.Get<int>(4);

    int handle = ConnectionManager::Instance().Open(host, user, password, database, port);
    return static_cast<cell>(handle);
}

// ============================================================
// pg_close(connection_handle)
// ============================================================
Plugin_Native(pg_close, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);

    ConnectionManager::Instance().Close(handle);
    return 1;
}

// ============================================================
// pg_query(connection_handle, query[], callback[], format[]="", {Float,_}:...)
// Async query with optional Pawn callback extra parameters.
// format[] chars: 'd'=int, 'f'=float, 's'=string
// ============================================================
Plugin_Native(pg_query, AMX *amx, cell *params)
{
    int num_params = static_cast<int>(params[0] / sizeof(cell));
    if (num_params < 3)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    std::string query = p.Get<std::string>(1);
    std::string callback = p.Get<std::string>(2);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn)
    {
        Log("[ERROR] pg_query: invalid connection handle %d", conn_handle);
        return 0;
    }

    // Parse optional format string + extra callback params (params[4..n])
    std::vector<CallbackParam> extra_params;
    if (num_params >= 4)
    {
        std::string fmt = p.Get<std::string>(3); // params[4] = format
        for (int i = 0; i < static_cast<int>(fmt.size()); ++i)
        {
            int raw_idx = 5 + i; // 1-based index into params[]
            if (raw_idx > num_params)
                break;

            CallbackParam cp;
            cp.type = fmt[i];
            // In Pawn ALL variadic args are passed by reference (AMX address).
            // We must dereference via Get_Addr even for scalars (int/float).
            cell *vaddr = nullptr;
            switch (fmt[i])
            {
            case 'f':
                if (amx::Get_Addr(amx, params[raw_idx], &vaddr) == 0 && vaddr)
                    cp.float_value = amx::AMX_CTOF(*vaddr);
                break;
            case 's':
                cp.str_value = Samp_SDK::Get_String(amx, params[raw_idx]);
                break;
            default: // 'd', 'i'
                if (amx::Get_Addr(amx, params[raw_idx], &vaddr) == 0 && vaddr)
                    cp.int_value = static_cast<int32_t>(*vaddr);
                break;
            }
            extra_params.push_back(std::move(cp));
        }
    }

    QueryTask task;
    task.conn_handle = conn_handle;
    task.query = std::move(query);
    task.callback = std::move(callback);
    task.extra_params = std::move(extra_params);

    ThreadPool::Instance().Enqueue(std::move(task));
    return 1;
}

// ============================================================
// pg_query_sync(connection_handle, query[])
// Synchronous query — blocks until done, returns result handle
// ============================================================
Plugin_Native(pg_query_sync, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 2)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    std::string query = p.Get<std::string>(1);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn)
    {
        Log("[ERROR] pg_query_sync: invalid connection handle %d", conn_handle);
        return 0;
    }

    PGresult *pg_res = nullptr;
    {
        std::lock_guard<std::mutex> lock(conn->conn_mutex);

        if (!conn->IsConnected())
        {
            if (!conn->Reconnect())
            {
                Log("[ERROR] pg_query_sync: could not reconnect handle %d",
                    conn_handle);
                return 0;
            }
        }

        pg_res = PQexec(conn->conn, query.c_str());
    }

    if (!pg_res)
    {
        Log("[ERROR] pg_query_sync: PQexec returned null (handle %d)",
            conn_handle);
        return 0;
    }

    ExecStatusType status = PQresultStatus(pg_res);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
    {
        Log("[ERROR] pg_query_sync error (handle %d): %s",
            conn_handle,
            PQresultErrorMessage(pg_res));
        PQclear(pg_res);
        return 0;
    }

    int result_handle = ResultManager::Instance().Store(pg_res);
    return static_cast<cell>(result_handle);
}

// ============================================================
// pg_num_rows(result_handle)
// ============================================================
Plugin_Native(pg_num_rows, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res)
        return 0;
    return static_cast<cell>(res->num_rows);
}

// ============================================================
// pg_num_fields(result_handle)
// ============================================================
Plugin_Native(pg_num_fields, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res)
        return 0;
    return static_cast<cell>(res->num_fields);
}

// ============================================================
// pg_get_field(result_handle, row, column, dest[], size)
// column can be int (index) or string (field name) — we handle int here
// Returns 1 on success, 0 on failure
// ============================================================
Plugin_Native(pg_get_field, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 5)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);
    int row = p.Get<int>(1);
    int col = p.Get<int>(2);
    int size = p.Get<int>(4);

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res || !res->pg_result)
        return 0;

    if (row < 0 || row >= res->num_rows)
        return 0;
    if (col < 0 || col >= res->num_fields)
        return 0;
    if (size <= 0)
        return 0;

    const char *value = PQgetisnull(res->pg_result, row, col)
                            ? ""
                            : PQgetvalue(res->pg_result, row, col);

    // Write into the Pawn output buffer referenced by params[4]
    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[4], &out_ptr) != 0 || !out_ptr)
    {
        return 0;
    }

    std::size_t len = std::strlen(value);
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (len < copy_len)
        copy_len = len;

    for (std::size_t i = 0; i < copy_len; ++i)
    {
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(value[i]));
    }
    out_ptr[copy_len] = 0;

    return 1;
}

// ============================================================
// pg_escape_string(connection_handle, input[], output[], size)
// Returns 1 on success
// ============================================================
Plugin_Native(pg_escape_string, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 4)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    int size = p.Get<int>(3);

    if (size <= 0)
        return 0;

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    std::string input = maybe_latin1_to_utf8(p.Get<std::string>(1),
                                             (conn ? conn->conn : nullptr));

    // Allocate escape buffer: at most 2*len+1 per libpq docs
    std::string escaped(input.size() * 2 + 1, '\0');
    std::size_t escaped_len = 0;

    if (conn && conn->conn)
    {
        int error = 0;
        escaped_len = PQescapeStringConn(conn->conn,
                                         &escaped[0],
                                         input.c_str(),
                                         input.size(),
                                         &error);
        if (error)
        {
            Log("[WARN] pg_escape_string: PQescapeStringConn error on handle %d",
                conn_handle);
        }
    }
    else
    {
        // Fallback without connection context
        escaped_len = PQescapeString(&escaped[0], input.c_str(), input.size());
    }

    escaped.resize(escaped_len);

    // Write to Pawn output buffer (params[3] = output[])
    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[3], &out_ptr) != 0 || !out_ptr)
    {
        return 0;
    }

    std::size_t copy_len = escaped_len;
    if (copy_len >= static_cast<std::size_t>(size))
    {
        copy_len = static_cast<std::size_t>(size) - 1;
    }

    for (std::size_t i = 0; i < copy_len; ++i)
    {
        out_ptr[i] = static_cast<cell>(
            static_cast<unsigned char>(escaped[i]));
    }
    out_ptr[copy_len] = 0;

    return 1;
}

// ============================================================
// pg_free_result(result_handle)
// Explicit result-set deallocation from Pawn
// ============================================================
Plugin_Native(pg_free_result, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);

    ResultManager::Instance().Free(handle);
    return 1;
}

// ============================================================
// ============================================================
//  PREPARED STATEMENTS
// ============================================================
// ============================================================

// ============================================================
// pg_prepare(connection_handle, name[], query[])
// Registers a named prepared statement on the server.
// Returns 1 on success, 0 on failure.
// ============================================================
Plugin_Native(pg_prepare, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 3)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    std::string name = p.Get<std::string>(1);
    std::string query = p.Get<std::string>(2);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn)
    {
        Log("[ERROR] pg_prepare: invalid connection handle %d", conn_handle);
        return 0;
    }

    PGresult *res = nullptr;
    {
        std::lock_guard<std::mutex> lock(conn->conn_mutex);
        if (!conn->IsConnected() && !conn->Reconnect())
        {
            Log("[ERROR] pg_prepare: could not connect (handle %d)", conn_handle);
            return 0;
        }
        res = PQprepare(conn->conn, name.c_str(), query.c_str(), 0, nullptr);
        if (res && PQresultStatus(res) == PGRES_COMMAND_OK)
            conn->prepared_stmts.insert(name);
    }

    if (!res)
        return 0;

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        Log("[ERROR] pg_prepare failed (handle %d, '%s'): %s",
            conn_handle, name.c_str(), PQresultErrorMessage(res));
        PQclear(res);
        return 0;
    }
    PQclear(res);
    return 1;
}

// ============================================================
// pg_exec_prepared(connection_handle, name[], callback[], format[]="", ...)
// Async execution of a named prepared statement.
// ============================================================
Plugin_Native(pg_exec_prepared, AMX *amx, cell *params)
{
    int num_params = static_cast<int>(params[0] / sizeof(cell));
    if (num_params < 3)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    std::string name = p.Get<std::string>(1);
    std::string callback = p.Get<std::string>(2);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn)
    {
        Log("[ERROR] pg_exec_prepared: invalid connection handle %d", conn_handle);
        return 0;
    }

    std::vector<CallbackParam> extra_params;
    if (num_params >= 4)
    {
        std::string fmt = p.Get<std::string>(3);
        for (int i = 0; i < static_cast<int>(fmt.size()); ++i)
        {
            int raw_idx = 5 + i;
            if (raw_idx > num_params)
                break;
            CallbackParam cp;
            cp.type = fmt[i];
            cell *vaddr = nullptr;
            switch (fmt[i])
            {
            case 'f':
                if (amx::Get_Addr(amx, params[raw_idx], &vaddr) == 0 && vaddr)
                    cp.float_value = amx::AMX_CTOF(*vaddr);
                break;
            case 's':
                cp.str_value = Samp_SDK::Get_String(amx, params[raw_idx]);
                break;
            default: // 'd', 'i'
                if (amx::Get_Addr(amx, params[raw_idx], &vaddr) == 0 && vaddr)
                    cp.int_value = static_cast<int32_t>(*vaddr);
                break;
            }
            extra_params.push_back(std::move(cp));
        }
    }

    QueryTask task;
    task.task_type = TaskType::PreparedQuery;
    task.conn_handle = conn_handle;
    task.stmt_name = std::move(name);
    task.callback = std::move(callback);
    task.extra_params = std::move(extra_params);

    ThreadPool::Instance().Enqueue(std::move(task));
    return 1;
}

// ============================================================
// pg_exec_prepared_sync(connection_handle, name[])
// Synchronous execution of a named prepared statement.
// Returns result handle, 0 on failure.
// ============================================================
Plugin_Native(pg_exec_prepared_sync, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 2)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    std::string name = p.Get<std::string>(1);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn)
    {
        Log("[ERROR] pg_exec_prepared_sync: invalid connection handle %d", conn_handle);
        return 0;
    }

    PGresult *pg_res = nullptr;
    {
        std::lock_guard<std::mutex> lock(conn->conn_mutex);
        if (!conn->IsConnected() && !conn->Reconnect())
        {
            Log("[ERROR] pg_exec_prepared_sync: reconnect failed (handle %d)", conn_handle);
            return 0;
        }
        pg_res = PQexecPrepared(conn->conn, name.c_str(),
                                0, nullptr, nullptr, nullptr, 0);
    }

    if (!pg_res)
        return 0;

    ExecStatusType status = PQresultStatus(pg_res);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
    {
        Log("[ERROR] pg_exec_prepared_sync error (handle %d, '%s'): %s",
            conn_handle, name.c_str(), PQresultErrorMessage(pg_res));
        PQclear(pg_res);
        return 0;
    }

    int rh = ResultManager::Instance().Store(pg_res);
    return static_cast<cell>(rh);
}

// ============================================================
// ============================================================
//  CACHE API
// ============================================================
// ============================================================

// ============================================================
// pg_cache_get_value(result, row, column_name[], dest[], size = sizeof dest)
// Retrieves a field by column name into dest[].
// ============================================================
Plugin_Native(pg_cache_get_value, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 5)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);
    int row = p.Get<int>(1);
    std::string col_name = p.Get<std::string>(2);
    // params[4] = dest[] address, params[5] = size
    int size = p.Get<int>(4);

    if (size <= 0)
        return 0;

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res || !res->pg_result)
        return 0;
    if (row < 0 || row >= res->num_rows)
        return 0;

    auto it = res->field_index.find(col_name);
    if (it == res->field_index.end())
        return 0;
    int col = it->second;

    const char *value = PQgetisnull(res->pg_result, row, col)
                            ? ""
                            : PQgetvalue(res->pg_result, row, col);

    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[4], &out_ptr) != 0 || !out_ptr)
        return 0;

    std::size_t len = std::strlen(value);
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (len < copy_len)
        copy_len = len;

    for (std::size_t i = 0; i < copy_len; ++i)
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(value[i]));
    out_ptr[copy_len] = 0;
    return 1;
}

// ============================================================
// pg_cache_get_value_index(result, row, column_index, dest[], size=sizeof dest)
// Retrieves a field by column index into dest[].
// ============================================================
Plugin_Native(pg_cache_get_value_index, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 5)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);
    int row = p.Get<int>(1);
    int col = p.Get<int>(2);
    // params[4] = dest[] address, params[5] = size
    int size = p.Get<int>(4);

    if (size <= 0)
        return 0;

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res || !res->pg_result)
        return 0;
    if (row < 0 || row >= res->num_rows)
        return 0;
    if (col < 0 || col >= res->num_fields)
        return 0;

    const char *value = PQgetisnull(res->pg_result, row, col)
                            ? ""
                            : PQgetvalue(res->pg_result, row, col);

    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[4], &out_ptr) != 0 || !out_ptr)
        return 0;

    std::size_t len = std::strlen(value);
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (len < copy_len)
        copy_len = len;

    for (std::size_t i = 0; i < copy_len; ++i)
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(value[i]));
    out_ptr[copy_len] = 0;
    return 1;
}

// ============================================================
// pg_cache_get_row_count(result)   -> int
// ============================================================
Plugin_Native(pg_cache_get_row_count, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;
    Native_Params p(amx, params);
    PgResult *res = ResultManager::Instance().Get(p.Get<int>(0));
    return res ? static_cast<cell>(res->num_rows) : 0;
}

// ============================================================
// pg_cache_get_field_count(result)   -> int
// ============================================================
Plugin_Native(pg_cache_get_field_count, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;
    Native_Params p(amx, params);
    PgResult *res = ResultManager::Instance().Get(p.Get<int>(0));
    return res ? static_cast<cell>(res->num_fields) : 0;
}

// ============================================================
// ============================================================
//  FIELD METADATA
// ============================================================
// ============================================================

// ============================================================
// pg_field_name(result, column_index, dest[], size = sizeof dest)
// Copies the column name at column_index into dest[].
// Returns 1 on success, 0 on failure.
// ============================================================
Plugin_Native(pg_field_name, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 4)
        return 0;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);
    int col = p.Get<int>(1);
    // params[3] = dest[] address, params[4] = size
    int size = p.Get<int>(3);

    if (size <= 0)
        return 0;

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res || !res->pg_result)
        return 0;
    if (col < 0 || col >= res->num_fields)
        return 0;

    const char *name = PQfname(res->pg_result, col);
    if (!name)
        return 0;

    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[3], &out_ptr) != 0 || !out_ptr)
        return 0;

    std::size_t len = std::strlen(name);
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (len < copy_len)
        copy_len = len;

    for (std::size_t i = 0; i < copy_len; ++i)
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(name[i]));
    out_ptr[copy_len] = 0;
    return 1;
}

// ============================================================
// pg_field_index(result, column_name[])
// Returns the zero-based column index for the given name, or -1.
// ============================================================
Plugin_Native(pg_field_index, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 2)
        return -1;

    Native_Params p(amx, params);
    int handle = p.Get<int>(0);
    std::string col_name = p.Get<std::string>(1);

    PgResult *res = ResultManager::Instance().Get(handle);
    if (!res || !res->pg_result)
        return -1;

    auto it = res->field_index.find(col_name);
    return (it != res->field_index.end()) ? static_cast<cell>(it->second) : -1;
}

// ============================================================
// ============================================================
//  TRANSACTION API
// ============================================================
// ============================================================

// Internal helper: run a blocking transaction command synchronously.
static cell ExecSyncCmd(int conn_handle, const char *cmd)
{
    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn)
    {
        PgPlugin::Log("[ERROR] %s: invalid connection handle %d", cmd, conn_handle);
        return 0;
    }
    PGresult *res = nullptr;
    {
        std::lock_guard<std::mutex> lock(conn->conn_mutex);
        if (!conn->IsConnected() && !conn->Reconnect())
        {
            PgPlugin::Log("[ERROR] %s: reconnect failed (handle %d)", cmd, conn_handle);
            return 0;
        }
        res = PQexec(conn->conn, cmd);
    }
    if (!res)
        return 0;
    ExecStatusType st = PQresultStatus(res);
    bool ok = (st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK);
    if (!ok)
        PgPlugin::Log("[ERROR] %s failed (handle %d): %s",
                      cmd, conn_handle, PQresultErrorMessage(res));
    PQclear(res);
    return ok ? 1 : 0;
}

// Internal helper: enqueue an async transaction command.
static cell EnqueueTx(int conn_handle, const char *cmd, const std::string &callback)
{
    if (!ConnectionManager::Instance().Get(conn_handle))
    {
        PgPlugin::Log("[ERROR] %s (async): invalid connection handle %d",
                      cmd, conn_handle);
        return 0;
    }
    QueryTask task;
    task.task_type = TaskType::Transaction;
    task.conn_handle = conn_handle;
    task.stmt_name = cmd;
    task.callback = callback;
    ThreadPool::Instance().Enqueue(std::move(task));
    return 1;
}

// pg_begin(connection_handle)
Plugin_Native(pg_begin, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;
    return ExecSyncCmd(Native_Params(amx, params).Get<int>(0), "BEGIN");
}

// pg_commit(connection_handle)
Plugin_Native(pg_commit, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;
    return ExecSyncCmd(Native_Params(amx, params).Get<int>(0), "COMMIT");
}

// pg_rollback(connection_handle)
Plugin_Native(pg_rollback, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;
    return ExecSyncCmd(Native_Params(amx, params).Get<int>(0), "ROLLBACK");
}

// pg_begin_async(connection_handle, callback[]="")
Plugin_Native(pg_begin_async, AMX *amx, cell *params)
{
    int n = static_cast<int>(params[0] / sizeof(cell));
    if (n < 1)
        return 0;
    Native_Params p(amx, params);
    std::string cb = (n >= 2) ? p.Get<std::string>(1) : "";
    return EnqueueTx(p.Get<int>(0), "BEGIN", cb);
}

// pg_commit_async(connection_handle, callback[]="")
Plugin_Native(pg_commit_async, AMX *amx, cell *params)
{
    int n = static_cast<int>(params[0] / sizeof(cell));
    if (n < 1)
        return 0;
    Native_Params p(amx, params);
    std::string cb = (n >= 2) ? p.Get<std::string>(1) : "";
    return EnqueueTx(p.Get<int>(0), "COMMIT", cb);
}

// pg_rollback_async(connection_handle, callback[]="")
Plugin_Native(pg_rollback_async, AMX *amx, cell *params)
{
    int n = static_cast<int>(params[0] / sizeof(cell));
    if (n < 1)
        return 0;
    Native_Params p(amx, params);
    std::string cb = (n >= 2) ? p.Get<std::string>(1) : "";
    return EnqueueTx(p.Get<int>(0), "ROLLBACK", cb);
}

// ============================================================
// ============================================================
//  ADVANCED ESCAPE FUNCTIONS
// ============================================================
// ============================================================

// ============================================================
// pg_escape_literal(connection_handle, input[], output[], size = sizeof output)
// Uses PQescapeLiteral (includes surrounding single-quotes).
// ============================================================
Plugin_Native(pg_escape_literal, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 4)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    // params[3] = output[] address, params[4] = size
    int size = p.Get<int>(3);

    if (size <= 0)
        return 0;

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn || !conn->conn)
    {
        Log("[ERROR] pg_escape_literal: invalid connection handle %d", conn_handle);
        return 0;
    }

    std::string input = maybe_latin1_to_utf8(p.Get<std::string>(1), conn->conn);
    char *escaped = PQescapeLiteral(conn->conn, input.c_str(), input.size());
    if (!escaped)
    {
        Log("[ERROR] pg_escape_literal failed (handle %d)", conn_handle);
        return 0;
    }

    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[3], &out_ptr) != 0 || !out_ptr)
    {
        PQfreemem(escaped);
        return 0;
    }

    std::size_t len = std::strlen(escaped);
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (len < copy_len)
        copy_len = len;

    for (std::size_t i = 0; i < copy_len; ++i)
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(escaped[i]));
    out_ptr[copy_len] = 0;

    PQfreemem(escaped);
    return 1;
}

// ============================================================
// pg_escape_identifier(connection_handle, input[], output[], size = sizeof output)
// Uses PQescapeIdentifier (includes surrounding double-quotes).
// ============================================================
Plugin_Native(pg_escape_identifier, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 4)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    // params[3] = output[] address, params[4] = size
    int size = p.Get<int>(3);

    if (size <= 0)
        return 0;

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn || !conn->conn)
    {
        Log("[ERROR] pg_escape_identifier: invalid connection handle %d", conn_handle);
        return 0;
    }

    std::string input = maybe_latin1_to_utf8(p.Get<std::string>(1), conn->conn);
    char *escaped = PQescapeIdentifier(conn->conn, input.c_str(), input.size());
    if (!escaped)
    {
        Log("[ERROR] pg_escape_identifier failed (handle %d)", conn_handle);
        return 0;
    }

    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[3], &out_ptr) != 0 || !out_ptr)
    {
        PQfreemem(escaped);
        return 0;
    }

    std::size_t len = std::strlen(escaped);
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (len < copy_len)
        copy_len = len;

    for (std::size_t i = 0; i < copy_len; ++i)
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(escaped[i]));
    out_ptr[copy_len] = 0;

    PQfreemem(escaped);
    return 1;
}

// ============================================================
// ============================================================
//  ASYNC CONNECT
// ============================================================
// ============================================================

// ============================================================
// pg_connect_async(host[], user[], password[], database[], port, callback[])
// Opens a connection on a background thread.
// When done, invokes: public callback(connection_handle)
//   connection_handle > 0 on success, 0 on failure.
// ============================================================
Plugin_Native(pg_connect_async, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 6)
        return 0;

    Native_Params p(amx, params);

    QueryTask task;
    task.task_type = TaskType::AsyncConnect;
    task.host = p.Get<std::string>(0);
    task.user = p.Get<std::string>(1);
    task.password = p.Get<std::string>(2);
    task.database = p.Get<std::string>(3);
    task.port = p.Get<int>(4);
    task.callback = p.Get<std::string>(5);

    ThreadPool::Instance().Enqueue(std::move(task));
    return 1;
}

// ============================================================
// ============================================================
//  PIPELINE MODE  (PostgreSQL 14+)
// ============================================================
// ============================================================

// ============================================================
// pg_enter_pipeline_mode(connection_handle)
// Puts the connection into pipeline mode (PQenterPipelineMode).
// Returns 1 on success, 0 on failure.
// ============================================================
Plugin_Native(pg_enter_pipeline_mode, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn || !conn->conn)
    {
        Log("[ERROR] pg_enter_pipeline_mode: invalid connection handle %d", conn_handle);
        return 0;
    }

    std::lock_guard<std::mutex> lock(conn->conn_mutex);
    if (PQenterPipelineMode(conn->conn) != 1)
    {
        Log("[ERROR] pg_enter_pipeline_mode failed (handle %d): %s",
            conn_handle, PQerrorMessage(conn->conn));
        return 0;
    }
    conn->pipeline_mode = true;
    return 1;
}

// ============================================================
// pg_exit_pipeline_mode(connection_handle)
// Exits pipeline mode (PQexitPipelineMode).
// Returns 1 on success, 0 on failure.
// ============================================================
Plugin_Native(pg_exit_pipeline_mode, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn || !conn->conn)
    {
        Log("[ERROR] pg_exit_pipeline_mode: invalid connection handle %d", conn_handle);
        return 0;
    }

    std::lock_guard<std::mutex> lock(conn->conn_mutex);
    if (PQexitPipelineMode(conn->conn) != 1)
    {
        Log("[ERROR] pg_exit_pipeline_mode failed (handle %d): %s",
            conn_handle, PQerrorMessage(conn->conn));
        return 0;
    }
    conn->pipeline_mode = false;
    return 1;
}

// ============================================================
// ============================================================
//  FORMAT HELPER
// ============================================================
// ============================================================

// ============================================================
// pg_format(connection_handle, output[], size, const format[], {Float,_}:...)
// Formats a SQL query string in-place, similar to mysql_format.
//
// Specifiers:
//   %d / %i  — integer
//   %f       — float
//   %s       — raw string (no escaping)
//   %e       — PQescapeLiteral  (escapes + wraps in single quotes)
//   %E       — PQescapeIdentifier (escapes + wraps in double quotes)
//   %%       — literal '%'
//
// Returns number of characters written (excluding null terminator),
// or -1 on error.
// ============================================================
Plugin_Native(pg_format, AMX *amx, cell *params)
{
    int num_params = static_cast<int>(params[0] / sizeof(cell));
    if (num_params < 4)
        return -1;

    Native_Params p(amx, params);
    int conn_handle = p.Get<int>(0);
    // params[2] = output[] address, params[3] = size, params[4] = format[]
    int size = p.Get<int>(2);
    std::string fmt = p.Get<std::string>(3);

    if (size <= 0)
        return -1;

    cell *out_ptr = nullptr;
    if (amx::Get_Addr(amx, params[2], &out_ptr) != 0 || !out_ptr)
        return -1;

    Connection *conn = ConnectionManager::Instance().Get(conn_handle);
    if (!conn || !conn->conn)
    {
        Log("[ERROR] pg_format: invalid connection handle %d", conn_handle);
        return -1;
    }

    // max useful result length is size-1 characters
    const std::size_t max_len = static_cast<std::size_t>(size - 1);

    std::string result;
    result.reserve(std::min<std::size_t>(fmt.size() * 2, max_len));

    int arg_idx = 0;
    int max_args = num_params - 4; // params[5..n] are varargs

    for (std::size_t i = 0; i < fmt.size() && result.size() < max_len; ++i)
    {
        if (fmt[i] != '%')
        {
            result += fmt[i];
            continue;
        }

        ++i; // advance to specifier character
        if (i >= fmt.size())
            break;

        char spec = fmt[i];

        if (spec == '%')
        {
            result += '%';
            continue;
        }

        if (arg_idx >= max_args)
        {
            Log("[WARN] pg_format: not enough arguments for format string (handle %d)", conn_handle);
            break;
        }

        // params[] is 1-based: params[1]=arg0 ... params[5]=first vararg
        int raw_idx = 5 + arg_idx;
        cell *vaddr = nullptr;

        switch (spec)
        {
        case 'd':
        case 'i':
            if (amx::Get_Addr(amx, params[raw_idx], &vaddr) == 0 && vaddr)
            {
                char buf[32];
                snprintf(buf, sizeof(buf), "%d", static_cast<int>(*vaddr));
                result += buf;
            }
            ++arg_idx;
            break;

        case 'f':
            if (amx::Get_Addr(amx, params[raw_idx], &vaddr) == 0 && vaddr)
            {
                float fval = amx::AMX_CTOF(*vaddr);
                // Guard against NaN/Infinity — they are not valid SQL literals
                if (fval != fval || fval == fval + 1.0f) // NaN or Inf check
                {
                    Log("[WARN] pg_format: NaN or Infinity passed as %%f arg %d (handle %d) — substituting 0.0",
                        arg_idx, conn_handle);
                    result += "0.0";
                }
                else
                {
                    char buf[64];
                    snprintf(buf, sizeof(buf), "%f", fval);
                    result += buf;
                }
            }
            ++arg_idx;
            break;

        case 's':
            // WARNING: %s performs NO escaping. Only use with trusted/hardcoded
            // strings. For player-provided input always use %e instead.
            {
                std::string sval = maybe_latin1_to_utf8(Samp_SDK::Get_String(amx, params[raw_idx]), conn->conn);
                result += sval;
                ++arg_idx;
                break;
            }

        case 'e': // PQescapeLiteral — escapes and wraps in single quotes
        {
            std::string sval = maybe_latin1_to_utf8(Samp_SDK::Get_String(amx, params[raw_idx]), conn->conn);
            char *escaped = PQescapeLiteral(conn->conn, sval.c_str(), sval.size());
            if (escaped)
            {
                result += escaped;
                PQfreemem(escaped);
            }
            else
            {
                Log("[WARN] pg_format: PQescapeLiteral failed for arg %d (handle %d)",
                    arg_idx, conn_handle);
                result += "''";
            }
            ++arg_idx;
            break;
        }

        case 'E': // PQescapeIdentifier — escapes and wraps in double quotes
        {
            std::string sval = maybe_latin1_to_utf8(Samp_SDK::Get_String(amx, params[raw_idx]), conn->conn);
            char *escaped = PQescapeIdentifier(conn->conn, sval.c_str(), sval.size());
            if (escaped)
            {
                result += escaped;
                PQfreemem(escaped);
            }
            else
            {
                Log("[WARN] pg_format: PQescapeIdentifier failed for arg %d (handle %d)",
                    arg_idx, conn_handle);
                result += "\"\"";
            }
            ++arg_idx;
            break;
        }

        default:
            Log("[WARN] pg_format: unknown specifier '%%%c' at position %zu (handle %d)",
                spec, i, conn_handle);
            result += '%';
            result += spec;
            break;
        }
    }

    // Write result into Pawn output buffer (cell by cell)
    std::size_t copy_len = static_cast<std::size_t>(size - 1);
    if (result.size() < copy_len)
        copy_len = result.size();

    for (std::size_t i = 0; i < copy_len; ++i)
        out_ptr[i] = static_cast<cell>(static_cast<unsigned char>(result[i]));
    out_ptr[copy_len] = 0;

    return static_cast<cell>(copy_len);
}

// ============================================================
// pg_set_charset_latin1(bool enable)
//
// Enables or disables automatic Latin-1 → UTF-8 re-encoding of
// all string arguments passed to pg_format (%s/%e/%E),
// pg_escape_string, pg_escape_literal and pg_escape_identifier.
//
// Enable this in OnGameModeInit when your server runs on a
// Linux SA-MP / open.mp instance that uses Latin-1 strings
// (the default) and your PostgreSQL cluster expects UTF-8
// (the default on Ubuntu 24+).
//
// @param  enable  1 to enable conversion, 0 to disable.
// @return         Previous state (1 = was enabled, 0 = was disabled).
// ============================================================
Plugin_Native(pg_set_charset_latin1, AMX *amx, cell *params)
{
    if (params[0] / sizeof(cell) < 1)
        return 0;

    bool enable = (Native_Params(amx, params).Get<int>(0) != 0);
    bool previous = g_latin1_convert.exchange(enable, std::memory_order_relaxed);
    Log("[INFO] pg_set_charset_latin1: Latin-1 to UTF-8 conversion %s",
        enable ? "enabled" : "disabled");
    return previous ? 1 : 0;
}
