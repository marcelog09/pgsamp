#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <cstdio>
#include <cstdarg>
#include <mutex>

namespace PgPlugin
{

    // -------------------------------------------------------
    // Extra parameter passed via the format string to callbacks.
    // type: 'd' = int32,  'f' = float,  's' = string
    // -------------------------------------------------------
    struct CallbackParam
    {
        char type = 'd';
        int32_t int_value = 0;
        float float_value = 0.0f;
        std::string str_value;
    };

    // -------------------------------------------------------
    // Server log forwarder — filled in by main.cpp on Load()
    // -------------------------------------------------------
    extern void (*g_logprintf)(const char *format, ...);

    // -------------------------------------------------------
    // Utility: log to server console AND to logs/postgres.log
    // -------------------------------------------------------
    void Log(const char *format, ...);

    // -------------------------------------------------------
    // Fire a Pawn public callback from the main thread.
    // Called by ThreadPool::DispatchPendingResults().
    // extra_params default = {} for backward-compatibility.
    // -------------------------------------------------------
    void FireCallback(const std::string &callback_name,
                      int result_handle,
                      const std::vector<CallbackParam> &extra_params = {});

    // -------------------------------------------------------
    // Initialise / shut-down the file logger
    // -------------------------------------------------------
    void Logger_Init(const char *path = "logs/plugins/postgres.log");
    void Logger_Shutdown();

} // namespace PgPlugin
