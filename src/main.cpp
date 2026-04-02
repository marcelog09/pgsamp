/*
 * main.cpp
 * Plugin entry-point.
 *
 * Defines SAMP_SDK_IMPLEMENTATION (one translation unit only).
 * Defines SAMP_SDK_WANT_AMX_EVENTS (to enable Plugin_Native auto-registration).
 * Defines SAMP_SDK_WANT_PROCESS_TICK (so we can dispatch async results).
 */

#define SAMP_SDK_IMPLEMENTATION
#define SAMP_SDK_WANT_AMX_EVENTS
#define SAMP_SDK_WANT_PROCESS_TICK

#include "../libs/samp-sdk/sdk/samp_sdk.hpp"
#include "../libs/samp-sdk/sdk/amx/amx_manager.hpp"

#include "postgres.h"
#include "postgres_internal.h"
#include "connection_manager.h"
#include "result.h"
#include "thread_pool.h"

#include <string>
#include <cstring>
#include <cstdint>

// ============================================================
// float_to_cell — reinterpret float bits as a Pawn cell (int32)
// ============================================================
static cell float_to_cell(float f)
{
    static_assert(sizeof(cell) == sizeof(float), "cell/float size mismatch");
    cell c;
    std::memcpy(&c, &f, sizeof(c));
    return c;
}

// ============================================================
// FireCallback — called from the main thread via DispatchPendingResults
// Pushes extra_params first (right-to-left), then result_handle,
// so the Pawn callback receives: public MyCallback(result_handle, p1, p2, ...)
// ============================================================
static void Impl_FireCallback(
    const std::string &name,
    int result_handle,
    const std::vector<PgPlugin::CallbackParam> &extra_params)
{
    int pub_index = -1;
    AMX *amx = Samp_SDK::Amx_Manager::Instance().Find_Public(name.c_str(), pub_index);

    if (!amx || pub_index < 0)
        return;

    cell hea_before = amx->hea;

    // Push extra params in reverse order (last param pushed first in AMX stack)
    for (int i = static_cast<int>(extra_params.size()) - 1; i >= 0; --i)
    {
        const PgPlugin::CallbackParam &cp = extra_params[i];
        switch (cp.type)
        {
        case 's':
        {
            cell amx_addr = 0;
            cell *amx_phys = nullptr;
            Samp_SDK::amx::Push_String(amx, &amx_addr, &amx_phys,
                                       cp.str_value.c_str());
            break;
        }
        case 'f':
            Samp_SDK::amx::Push(amx, float_to_cell(cp.float_value));
            break;
        default: // 'd', 'i'
            Samp_SDK::amx::Push(amx, static_cast<cell>(cp.int_value));
            break;
        }
    }

    // result_handle is params[1] in Pawn — pushed last
    Samp_SDK::amx::Push(amx, static_cast<cell>(result_handle));

    cell retval = 0;
    Samp_SDK::amx::Exec(amx, &retval, pub_index);

    // Restore heap; this releases any strings pushed above
    amx->hea = hea_before;
}

// ============================================================
// Plugin lifecycle
// ============================================================

bool OnLoad()
{
    // Grab logprintf through the SDK utility
    PgPlugin::g_logprintf = reinterpret_cast<void (*)(const char *, ...)>(
        Samp_SDK::Core::Instance().Get_Plugin_Data()[PLUGIN_DATA_LOGPRINTF]);

    // Register the FireCallback bridge
    PgPlugin_RegisterFireCallback(&Impl_FireCallback);

    // Initialise file logger
    PgPlugin::Logger_Init("logs/plugins/postgres.log");

    // Start the thread pool (singleton constructed on first access)
    // Accessing it here ensures workers are alive before any native call
    (void)PgPlugin::ThreadPool::Instance();

    // Start background keepalive — pings all connections every 60 s
    PgPlugin::ConnectionManager::Instance().StartKeepalive();

    PgPlugin::Log("PostgreSQL plugin loaded. Version 2.0.0 (SA-MP / open.mp)");
    return true;
}

void OnUnload()
{
    PgPlugin::Log("PostgreSQL plugin unloading...");

    // Stop keepalive before closing connections
    PgPlugin::ConnectionManager::Instance().StopKeepalive();

    // Stop thread pool first (joins all worker threads)
    PgPlugin::ThreadPool::Instance().Shutdown();

    // Close all DB connections
    PgPlugin::ConnectionManager::Instance().CloseAll();

    // Free all cached results
    PgPlugin::ResultManager::Instance().FreeAll();

    PgPlugin::Logger_Shutdown();
}

unsigned int GetSupportFlags()
{
    return SUPPORTS_VERSION;
}

void OnAmxLoad(AMX *amx)
{
    // Register with Amx_Manager so Call_Public can find publics in this AMX.
    Samp_SDK::Amx_Manager::Instance().Add_Amx(amx);
}

void OnAmxUnload(AMX *amx)
{
    Samp_SDK::Amx_Manager::Instance().Remove_Amx(amx);
}

// ============================================================
// ProcessTick — dispatch async results back to Pawn every tick
// ============================================================
void OnProcessTick()
{
    PgPlugin::ThreadPool::Instance().DispatchPendingResults();
}
