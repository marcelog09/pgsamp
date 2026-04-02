#include "connection_manager.h"
#include "postgres.h"

#include <libpq-fe.h>
#include <sstream>
#include <cstring>

namespace PgPlugin
{

    // ============================================================
    // Connection
    // ============================================================

    Connection::~Connection()
    {
        if (conn)
        {
            PQfinish(conn);
            conn = nullptr;
        }
    }

    bool Connection::Connect()
    {
        // Build connection string without storing the password in a reachable var
        std::ostringstream cs;
        cs << "host=" << info.host
           << " user=" << info.user
           << " password=" << info.password
           << " dbname=" << info.database
           << " port=" << info.port
           << " connect_timeout=10";

        conn = PQconnectdb(cs.str().c_str());

        if (PQstatus(conn) != CONNECTION_OK)
        {
            PgPlugin::Log("[ERROR] Connection to database failed: %s",
                          PQerrorMessage(conn));
            PQfinish(conn);
            conn = nullptr;
            return false;
        }

        PgPlugin::Log("[INFO] Connected to PostgreSQL (handle %d): host=%s db=%s",
                      handle_id,
                      info.host.c_str(),
                      info.database.c_str());
        return true;
    }

    bool Connection::IsConnected()
    {
        if (!conn)
            return false;
        return PQstatus(conn) == CONNECTION_OK;
    }

    bool Connection::Reconnect()
    {
        // Guard against simultaneous reconnect attempts
        bool expected = false;
        if (!reconnecting.compare_exchange_strong(expected, true))
        {
            return false; // Already reconnecting
        }

        PgPlugin::Log("[WARN] Connection %d lost. Attempting reconnection...",
                      handle_id);

        // Try PQreset first (cheaper)
        if (conn)
        {
            PQreset(conn);
            if (PQstatus(conn) == CONNECTION_OK)
            {
                PgPlugin::Log("[INFO] Reconnected (handle %d) via PQreset.",
                              handle_id);
                reconnecting = false;
                return true;
            }
            PQfinish(conn);
            conn = nullptr;
        }

        // Fallback: full reconnect
        bool ok = Connect();

        if (!ok)
        {
            PgPlugin::Log("[ERROR] Reconnection failed for handle %d.", handle_id);
        }
        else
        {
            PgPlugin::Log("[INFO] Reconnection succeeded for handle %d.",
                          handle_id);
        }

        reconnecting = false;
        return ok;
    }

    // ============================================================
    // ConnectionManager
    // ============================================================

    ConnectionManager &ConnectionManager::Instance()
    {
        static ConnectionManager instance;
        return instance;
    }

    int ConnectionManager::Open(const std::string &host,
                                const std::string &user,
                                const std::string &password,
                                const std::string &database,
                                int port)
    {
        auto conn_obj = std::make_unique<Connection>();
        conn_obj->info.host = host;
        conn_obj->info.user = user;
        conn_obj->info.password = password;
        conn_obj->info.database = database;
        conn_obj->info.port = port;

        // Reserve the handle atomically before connecting, so concurrent Open()
        // calls always get distinct handle IDs (avoids TOCTOU race).
        int id;
        {
            std::lock_guard<std::mutex> lock(map_mutex_);
            id = next_handle_++;
        }
        conn_obj->handle_id = id;

        if (!conn_obj->Connect())
        {
            return 0;
        }

        std::lock_guard<std::mutex> lock(map_mutex_);
        connections_[id] = std::move(conn_obj);
        ++map_generation_;
        return id;
    }

    void ConnectionManager::Close(int handle)
    {
        std::lock_guard<std::mutex> lock(map_mutex_);
        connections_.erase(handle);
        ++map_generation_;
    }

    Connection *ConnectionManager::Get(int handle)
    {
        std::lock_guard<std::mutex> lock(map_mutex_);
        auto it = connections_.find(handle);
        if (it == connections_.end())
            return nullptr;
        return it->second.get();
    }

    void ConnectionManager::CloseAll()
    {
        std::lock_guard<std::mutex> lock(map_mutex_);
        connections_.clear();
        ++map_generation_;
    }

    // ============================================================
    // Keepalive
    // ============================================================

    void ConnectionManager::StartKeepalive()
    {
        keepalive_stop_ = false;
        keepalive_thread_ = std::thread(&ConnectionManager::KeepaliveWorker, this);
    }

    void ConnectionManager::StopKeepalive()
    {
        {
            std::lock_guard<std::mutex> lk(keepalive_mutex_);
            keepalive_stop_ = true;
        }
        keepalive_cv_.notify_all();
        if (keepalive_thread_.joinable())
            keepalive_thread_.join();
    }

    void ConnectionManager::KeepaliveWorker()
    {
        while (true)
        {
            // Wait 60 seconds or until signalled to stop
            {
                std::unique_lock<std::mutex> lk(keepalive_mutex_);
                keepalive_cv_.wait_for(lk, std::chrono::seconds(60),
                                       [this]
                                       { return keepalive_stop_.load(); });
            }
            if (keepalive_stop_)
                break;

            // Snapshot raw pointers while holding map_mutex_ briefly.
            // Storing raw pointers is safe because:
            //   (a) we check map_generation_ before using each pointer, and
            //   (b) we use try_lock so we never block while holding map_mutex_.
            std::vector<std::pair<int, Connection *>> snap;
            int gen = 0;
            {
                std::lock_guard<std::mutex> ml(map_mutex_);
                gen = map_generation_;
                for (auto &kv : connections_)
                    snap.emplace_back(kv.first, kv.second.get());
            }

            for (auto &[id, raw] : snap)
            {
                // Abort the round if the map has been modified since snapshot
                {
                    std::lock_guard<std::mutex> ml(map_mutex_);
                    if (map_generation_ != gen)
                        break;
                }

                // Non-blocking: skip busy connections rather than deadlock
                std::unique_lock<std::mutex> cl(raw->conn_mutex, std::try_to_lock);
                if (!cl.owns_lock())
                    continue;

                if (raw->conn && PQstatus(raw->conn) == CONNECTION_OK)
                {
                    PGresult *res = PQexec(raw->conn, "SELECT 1");
                    if (res)
                        PQclear(res);
                }
            }
        }
    }

} // namespace PgPlugin
