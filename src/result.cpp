#include "result.h"

#include <libpq-fe.h>
#include <cstring>
#include <mutex>
#include <utility>

namespace PgPlugin
{

    // ============================================================
    // PgResult
    // ============================================================

    PgResult::~PgResult()
    {
        Clear();
    }

    PgResult::PgResult(PgResult &&other) noexcept
        : handle_id(other.handle_id), pg_result(other.pg_result), num_rows(other.num_rows), num_fields(other.num_fields), field_index(std::move(other.field_index))
    {
        other.pg_result = nullptr;
        other.handle_id = 0;
        other.num_rows = 0;
        other.num_fields = 0;
    }

    PgResult &PgResult::operator=(PgResult &&other) noexcept
    {
        if (this != &other)
        {
            Clear();
            handle_id = other.handle_id;
            pg_result = other.pg_result;
            num_rows = other.num_rows;
            num_fields = other.num_fields;
            field_index = std::move(other.field_index);
            other.pg_result = nullptr;
            other.handle_id = 0;
            other.num_rows = 0;
            other.num_fields = 0;
        }
        return *this;
    }

    void PgResult::Clear()
    {
        if (pg_result)
        {
            PQclear(pg_result);
            pg_result = nullptr;
        }
        field_index.clear();
        num_rows = num_fields = 0;
    }

    void PgResult::BuildFieldIndex()
    {
        if (!pg_result)
            return;
        field_index.clear();
        for (int i = 0; i < num_fields; ++i)
        {
            const char *name = PQfname(pg_result, i);
            if (name)
            {
                field_index[name] = i;
            }
        }
    }

    // ============================================================
    // ResultManager
    // ============================================================

    ResultManager &ResultManager::Instance()
    {
        static ResultManager instance;
        return instance;
    }

    int ResultManager::Store(PGresult *pg_res)
    {
        if (!pg_res)
            return 0;

        auto result = std::make_unique<PgResult>();
        result->pg_result = pg_res;
        result->num_rows = PQntuples(pg_res);
        result->num_fields = PQnfields(pg_res);
        result->BuildFieldIndex();

        std::lock_guard<std::mutex> lock(mutex_);
        int id = next_handle_++;
        result->handle_id = id;
        results_[id] = std::move(result);
        return id;
    }

    PgResult *ResultManager::Get(int handle)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = results_.find(handle);
        if (it == results_.end())
            return nullptr;
        return it->second.get();
    }

    void ResultManager::Free(int handle)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        results_.erase(handle);
    }

    void ResultManager::FreeAll()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        results_.clear();
    }

} // namespace PgPlugin
