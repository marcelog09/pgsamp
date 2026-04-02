#pragma once

#include "postgres.h" // CallbackParam

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>
#include <string>

namespace PgPlugin
{

    // ------------------------------------------------------------------
    // Discriminates what a QueryTask represents inside the thread pool.
    // ------------------------------------------------------------------
    enum class TaskType
    {
        Query,         ///< Regular parameterless SQL query  (PQsendQuery)
        PreparedQuery, ///< Execute a named prepared statement (PQsendQueryPrepared)
        Transaction,   ///< BEGIN / COMMIT / ROLLBACK          (PQexec)
        AsyncConnect   ///< Non-blocking connection open
    };

    // ------------------------------------------------------------------
    // Task submitted to the thread pool via ThreadPool::Enqueue().
    // ------------------------------------------------------------------
    struct QueryTask
    {
        TaskType task_type = TaskType::Query;
        int conn_handle = 0;

        std::string query;    ///< SQL text (Query tasks)
        std::string callback; ///< Pawn public to invoke on completion

        /// Extra parameters forwarded verbatim to the Pawn callback.
        std::vector<CallbackParam> extra_params;

        /// PreparedQuery: statement name.
        /// Transaction  : command string ("BEGIN" / "COMMIT" / "ROLLBACK").
        std::string stmt_name;

        // AsyncConnect fields ----------------------------------------
        std::string host;
        std::string user;
        std::string password;
        std::string database;
        int port = 5432;
    };

    // ------------------------------------------------------------------
    // Result returned from a worker to the main-thread dispatch queue.
    // ------------------------------------------------------------------
    struct QueryResult
    {
        TaskType task_type = TaskType::Query;
        int result_handle = 0; ///< 0 = error / no result set
        std::string callback;
        bool success = false;

        /// Mirrors QueryTask::extra_params — forwarded to FireCallback.
        std::vector<CallbackParam> extra_params;

        /// AsyncConnect: the new connection handle (0 = failure).
        int conn_handle = 0;
    };

    // ------------------------------------------------------------------
    class ThreadPool
    {
    public:
        explicit ThreadPool(std::size_t num_threads);
        ~ThreadPool();

        // Non-copyable
        ThreadPool(const ThreadPool &) = delete;
        ThreadPool &operator=(const ThreadPool &) = delete;

        // Enqueue an async task
        void Enqueue(QueryTask task);

        // Stop all workers gracefully (called from OnUnload)
        void Shutdown();

        // Called from OnProcessTick — dispatches finished results back to Pawn
        void DispatchPendingResults();

        static ThreadPool &Instance();

    private:
        void WorkerThread();

        // Input queue (C++ tasks)
        std::queue<QueryTask> task_queue_;
        std::mutex task_mutex_;
        std::condition_variable task_cv_;

        // Output queue (results ready for Pawn callback)
        std::queue<QueryResult> result_queue_;
        std::mutex result_mutex_;

        std::vector<std::thread> workers_;
        std::atomic<bool> stop_{false};
    };

} // namespace PgPlugin
