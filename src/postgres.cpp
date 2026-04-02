#include "postgres.h"

#include <ctime>
#include <cstring>
#include <cstdio>

#if defined(_WIN32)
#include <direct.h>
#define MKDIR(p) _mkdir(p)
#else
#include <sys/stat.h>
#define MKDIR(p) mkdir((p), 0755)
#endif

// These are provided by main.cpp via the SA-MP SDK
// Declared extern so thread_pool.cpp (and others) can reach them.
namespace PgPlugin
{

    void (*g_logprintf)(const char *format, ...) = nullptr;

    // ============================================================
    // File logger
    // ============================================================
    namespace
    {
        FILE *g_log_file = nullptr;
        std::mutex g_log_mutex;
    }

    void Logger_Init(const char *path)
    {
        std::lock_guard<std::mutex> lock(g_log_mutex);

        // Create logs/ directory if needed
        MKDIR("logs");

        g_log_file = std::fopen(path, "a");
        if (!g_log_file)
        {
            // Continue without file logging — server console is still used
        }
    }

    void Logger_Shutdown()
    {
        std::lock_guard<std::mutex> lock(g_log_mutex);
        if (g_log_file)
        {
            std::fclose(g_log_file);
            g_log_file = nullptr;
        }
    }

    // ============================================================
    // Log: writes to both server console and postgres.log
    // ============================================================
    void Log(const char *format, ...)
    {
        // Build message
        char buf[2048];
        va_list args;
        va_start(args, format);
        std::vsnprintf(buf, sizeof(buf), format, args);
        va_end(args);

        // Timestamp — use thread-safe variants to avoid data races on the
        // static buffer returned by std::localtime.
        time_t now = std::time(nullptr);
        char tbuf[32];
        struct tm tm_info;
#if defined(_WIN32)
        localtime_s(&tm_info, &now);
#else
        localtime_r(&now, &tm_info);
#endif
        std::strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", &tm_info);

        // Server console
        if (g_logprintf)
        {
            g_logprintf("[PGPlugin] %s", buf);
        }

        // File
        std::lock_guard<std::mutex> lock(g_log_mutex);
        if (g_log_file)
        {
            std::fprintf(g_log_file, "[%s] %s\n", tbuf, buf);
            std::fflush(g_log_file);
        }
    }

    // ============================================================
    // FireCallback — called from main thread (DispatchPendingResults)
    // actual Pawn_Public call lives in main.cpp to avoid circular deps
    // We use a registered function pointer.
    // ============================================================
    namespace
    {
        void (*g_fire_cb)(const std::string &, int,
                          const std::vector<CallbackParam> &) = nullptr;
    }

} // anonymous namespace

// Public setter called from main.cpp
void PgPlugin_RegisterFireCallback(
    void (*fn)(const std::string &, int,
               const std::vector<PgPlugin::CallbackParam> &))
{
    PgPlugin::g_fire_cb = fn;
}

namespace PgPlugin
{

    void FireCallback(const std::string &callback_name, int result_handle,
                      const std::vector<CallbackParam> &extra_params)
    {
        if (g_fire_cb)
        {
            g_fire_cb(callback_name, result_handle, extra_params);
        }
    }

} // namespace PgPlugin
