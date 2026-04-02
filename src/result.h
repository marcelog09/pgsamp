#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>

// Forward-declare PGresult to avoid including libpq-fe.h in the header
struct pg_result;
typedef struct pg_result PGresult;

namespace PgPlugin
{

    struct PgResult
    {
        int handle_id = 0;
        PGresult *pg_result = nullptr;
        int num_rows = 0;
        int num_fields = 0;

        // Field name -> column index cache
        std::unordered_map<std::string, int> field_index;

        PgResult() = default;
        ~PgResult();

        // Non-copyable, movable
        PgResult(const PgResult &) = delete;
        PgResult &operator=(const PgResult &) = delete;
        PgResult(PgResult &&other) noexcept;
        PgResult &operator=(PgResult &&other) noexcept;

        void BuildFieldIndex();
        void Clear();
    };

    class ResultManager
    {
    public:
        static ResultManager &Instance();

        // Store a result and return its handle ID
        int Store(PGresult *pg_res);

        // Retrieve by handle
        PgResult *Get(int handle);

        // Free a result by handle
        void Free(int handle);

        // Free all results (on shutdown)
        void FreeAll();

    private:
        ResultManager() = default;
        ~ResultManager() = default;

        int next_handle_ = 1;
        std::unordered_map<int, std::unique_ptr<PgResult>> results_;
        std::mutex mutex_;
    };

} // namespace PgPlugin
