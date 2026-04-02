#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <vector>

// Forward declarations
struct pg_conn;
typedef struct pg_conn PGconn;

namespace PgPlugin
{

    struct ConnectionInfo
    {
        std::string host;
        std::string user;
        std::string password;
        std::string database;
        int port = 5432;
    };

    struct Connection
    {
        int handle_id = 0;
        PGconn *conn = nullptr;
        ConnectionInfo info;
        std::mutex conn_mutex; // Serialize access per connection
        std::atomic<bool> reconnecting{false};

        /// Names of prepared statements registered on this connection.
        std::unordered_set<std::string> prepared_stmts;

        /// True when the connection is in PostgreSQL pipeline mode.
        std::atomic<bool> pipeline_mode{false};

        Connection() = default;
        ~Connection();

        // Non-copyable, non-movable (mutex & atomic require this)
        Connection(const Connection &) = delete;
        Connection &operator=(const Connection &) = delete;
        Connection(Connection &&) = delete;
        Connection &operator=(Connection &&) = delete;

        bool Connect();
        bool IsConnected();
        bool Reconnect();
    };

    class ConnectionManager
    {
    public:
        static ConnectionManager &Instance();

        // Open a new connection and return its handle (0 = failure)
        int Open(const std::string &host,
                 const std::string &user,
                 const std::string &password,
                 const std::string &database,
                 int port);

        // Close a connection by handle
        void Close(int handle);

        // Retrieve a connection pointer (nullptr if not found)
        Connection *Get(int handle);

        // Close all connections (on shutdown)
        void CloseAll();

        // Start/stop the background keepalive thread (called from main.cpp)
        void StartKeepalive();
        void StopKeepalive();

    private:
        ConnectionManager() = default;
        ~ConnectionManager() = default;

        // Worker function executed by keepalive_thread_
        void KeepaliveWorker();

        int next_handle_ = 1;
        int map_generation_ = 0; // bumped on every add/remove for safe snapshots

        std::mutex map_mutex_;
        std::unordered_map<int, std::unique_ptr<Connection>> connections_;

        // Keepalive thread members
        std::thread keepalive_thread_;
        std::atomic<bool> keepalive_stop_{false};
        std::condition_variable keepalive_cv_;
        std::mutex keepalive_mutex_;
    };

} // namespace PgPlugin
