#pragma once

#include "postgres.h"

// Forward declaration for the FireCallback registration
// (defined in postgres.cpp, called from main.cpp)
void PgPlugin_RegisterFireCallback(
    void (*fn)(const std::string &, int,
               const std::vector<PgPlugin::CallbackParam> &));
