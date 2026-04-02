#include "thread_pool.h"
#include "connection_manager.h"
#include "result.h"
#include "postgres.h"

#include <libpq-fe.h>

namespace PgPlugin
{

    ThreadPool &ThreadPool::Instance()
    {
        // Default 4 worker threads; can be extended via config
        static ThreadPool instance(4);
        return instance;
    }

    ThreadPool::ThreadPool(std::size_t num_threads)
    {
        for (std::size_t i = 0; i < num_threads; ++i)
            workers_.emplace_back(&ThreadPool::WorkerThread, this);
    }

    ThreadPool::~ThreadPool()
    {
        Shutdown();
    }

    void ThreadPool::Shutdown()
    {
        if (stop_)
            return;
        {
            std::lock_guard<std::mutex> lock(task_mutex_);
            stop_ = true;
        }
        task_cv_.notify_all();

        for (auto &t : workers_)
        {
            if (t.joinable())
                t.join();
        }
        workers_.clear();
    }

    void ThreadPool::Enqueue(QueryTask task)
    {
        {
            std::lock_guard<std::mutex> lock(task_mutex_);
            task_queue_.push(std::move(task));
        }
        task_cv_.notify_one();
    }

    // ================================================================
    // Worker: runs on background threads.
    // ================================================================
    void ThreadPool::WorkerThread()
    {
        while (true)
        {
            QueryTask task;
            {
                std::unique_lock<std::mutex> lock(task_mutex_);
                task_cv_.wait(lock, [this]
                              { return stop_ || !task_queue_.empty(); });

                if (stop_ && task_queue_.empty())
                    return;

                task = std::move(task_queue_.front());
                task_queue_.pop();
            }

            // Skeleton result — callback name and extra params always propagate
            QueryResult qr;
            qr.task_type = task.task_type;
            qr.callback = task.callback;
            qr.extra_params = std::move(task.extra_params);

            // --------------------------------------------------------
            // AsyncConnect: open the connection on the worker thread
            // --------------------------------------------------------
            if (task.task_type == TaskType::AsyncConnect)
            {
                int h = ConnectionManager::Instance().Open(
                    task.host, task.user, task.password, task.database, task.port);
                qr.conn_handle = h;
                qr.result_handle = h; // callback receives conn handle as first arg
                qr.success = (h != 0);
            }
            else
            {
                Connection *conn = ConnectionManager::Instance().Get(task.conn_handle);

                if (!conn)
                {
                    Log("[ERROR] Worker: invalid connection handle %d", task.conn_handle);
                    // qr already has result_handle=0, success=false
                }

                // ----------------------------------------------------
                // Transaction: BEGIN / COMMIT / ROLLBACK
                // ----------------------------------------------------
                else if (task.task_type == TaskType::Transaction)
                {
                    std::lock_guard<std::mutex> cl(conn->conn_mutex);

                    if (!conn->IsConnected() && !conn->Reconnect())
                    {
                        Log("[ERROR] Async tx '%s': reconnect failed (handle %d)",
                            task.stmt_name.c_str(), task.conn_handle);
                    }
                    else
                    {
                        PGresult *res = PQexec(conn->conn, task.stmt_name.c_str());
                        if (!res)
                        {
                            Log("[ERROR] Async tx '%s' (handle %d): PQexec returned null",
                                task.stmt_name.c_str(), task.conn_handle);
                        }
                        else
                        {
                            ExecStatusType st = PQresultStatus(res);
                            qr.success = (st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK);
                            if (!qr.success)
                                Log("[ERROR] Async tx '%s' (handle %d): %s",
                                    task.stmt_name.c_str(), task.conn_handle,
                                    PQresultErrorMessage(res));
                            qr.result_handle = qr.success ? 1 : 0;
                            PQclear(res);
                        }
                    }
                }

                // ----------------------------------------------------
                // PreparedQuery: PQsendQueryPrepared + PQgetResult loop
                // ----------------------------------------------------
                else if (task.task_type == TaskType::PreparedQuery)
                {
                    PGresult *pg_res = nullptr;
                    {
                        std::lock_guard<std::mutex> cl(conn->conn_mutex);

                        if (!conn->IsConnected() && !conn->Reconnect())
                        {
                            Log("[ERROR] pg_exec_prepared async: reconnect failed (handle %d)",
                                task.conn_handle);
                        }
                        else if (PQsendQueryPrepared(conn->conn,
                                                     task.stmt_name.c_str(),
                                                     0, nullptr, nullptr, nullptr, 0) == 1)
                        {
                            PGresult *tmp = nullptr;
                            while ((tmp = PQgetResult(conn->conn)) != nullptr)
                            {
                                if (pg_res)
                                    PQclear(pg_res);
                                pg_res = tmp;
                            }
                        }
                        else
                        {
                            Log("[ERROR] PQsendQueryPrepared failed (handle %d, stmt '%s'): %s",
                                task.conn_handle, task.stmt_name.c_str(),
                                PQerrorMessage(conn->conn));
                        }
                    }

                    if (pg_res)
                    {
                        ExecStatusType st = PQresultStatus(pg_res);
                        if (st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK)
                        {
                            qr.result_handle = ResultManager::Instance().Store(pg_res);
                            qr.success = (qr.result_handle != 0);
                            // ownership transferred to ResultManager — do NOT PQclear here
                        }
                        else
                        {
                            Log("[ERROR] PreparedQuery error (handle %d, stmt '%s'): %s",
                                task.conn_handle, task.stmt_name.c_str(),
                                PQresultErrorMessage(pg_res));
                            PQclear(pg_res);
                        }
                    }
                }

                // ----------------------------------------------------
                // Query (default): PQsendQuery + PQgetResult loop
                // ----------------------------------------------------
                else
                {
                    PGresult *pg_res = nullptr;
                    {
                        std::lock_guard<std::mutex> cl(conn->conn_mutex);

                        if (!conn->IsConnected())
                        {
                            if (!conn->Reconnect())
                            {
                                Log("[ERROR] Async query (handle %d): could not reconnect.",
                                    task.conn_handle);
                                goto push_result;
                            }
                        }

                        // PQsendQueryParams (0 params) is used instead of PQsendQuery
                        // because PQsendQuery is forbidden in pipeline mode (PG 14+).
                        if (PQsendQueryParams(conn->conn, task.query.c_str(),
                                              0, nullptr, nullptr, nullptr, nullptr, 0) != 1)
                        {
                            Log("[ERROR] PQsendQueryParams failed (handle %d): %s",
                                task.conn_handle, PQerrorMessage(conn->conn));
                            goto push_result;
                        }

                        // In pipeline mode the server only sends results after a
                        // PQpipelineSync flush. Without it PQgetResult blocks forever.
                        if (conn->pipeline_mode.load())
                        {
                            if (PQpipelineSync(conn->conn) != 1)
                            {
                                Log("[ERROR] PQpipelineSync failed (handle %d): %s",
                                    task.conn_handle, PQerrorMessage(conn->conn));
                                goto push_result;
                            }
                        }

                        // In pipeline mode the result sequence for "1 query + PQpipelineSync" is:
                        //   PGRES_TUPLES_OK  → NULL  → PGRES_PIPELINE_SYNC  → NULL
                        // The inner loop below exits at the first NULL (after the real result).
                        // The outer loop then drains PGRES_PIPELINE_SYNC + final NULL so that
                        // the connection's asyncStatus returns to PGASYNC_IDLE, making
                        // PQexitPipelineMode succeed later.
                        PGresult *tmp = nullptr;

                        // First pass: collect the actual query result (exits at first NULL).
                        while ((tmp = PQgetResult(conn->conn)) != nullptr)
                        {
                            if (pg_res)
                                PQclear(pg_res);
                            pg_res = tmp;
                        }

                        // Second pass (pipeline only): drain PGRES_PIPELINE_SYNC + final NULL.
                        if (conn->pipeline_mode.load())
                        {
                            while ((tmp = PQgetResult(conn->conn)) != nullptr)
                                PQclear(tmp);
                        }
                    }

                    if (pg_res)
                    {
                        ExecStatusType st = PQresultStatus(pg_res);
                        if (st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK)
                        {
                            qr.result_handle = ResultManager::Instance().Store(pg_res);
                            qr.success = (qr.result_handle != 0);
                        }
                        else
                        {
                            Log("[ERROR] Query error (handle %d): %s",
                                task.conn_handle, PQresultErrorMessage(pg_res));
                            PQclear(pg_res);
                        }
                    }
                }
            }

        push_result:
        {
            std::lock_guard<std::mutex> rlock(result_mutex_);
            result_queue_.push(std::move(qr));
        }
        }
    }

    // ================================================================
    // Dispatch: called from main thread (OnProcessTick)
    // ================================================================
    void ThreadPool::DispatchPendingResults()
    {
        std::queue<QueryResult> local;
        {
            std::lock_guard<std::mutex> rlock(result_mutex_);
            std::swap(local, result_queue_);
        }

        while (!local.empty())
        {
            QueryResult &qr = local.front();

            if (!qr.callback.empty())
                PgPlugin::FireCallback(qr.callback, qr.result_handle, qr.extra_params);

            local.pop();
        }
    }

} // namespace PgPlugin
