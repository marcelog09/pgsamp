cmake_minimum_required(VERSION 3.16)
project(pgsamp VERSION 1.0.0 LANGUAGES CXX)

# ===========================================================================
# C++ standard
# ===========================================================================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ===========================================================================
# Architecture: MUST be 32-bit for SA-MP compatibility
# ===========================================================================
if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR
        "SA-MP requires 32-bit plugins.\n"
        "  Windows (MSVC)  : Set the solution platform to x86.\n"
        "  Linux / MinGW   : Pass -DCMAKE_C_FLAGS=-m32 -DCMAKE_CXX_FLAGS=-m32\n"
        "                    or use a cross-compilation toolchain.")
endif()

# ===========================================================================
# Source files
# ===========================================================================
set(PLUGIN_SOURCES
    src/main.cpp
    src/natives.cpp
    src/postgres.cpp
    src/connection_manager.cpp
    src/result.cpp
    src/thread_pool.cpp
)

# ===========================================================================
# Shared library target (.dll / .so)
# ===========================================================================
add_library(pgsamp SHARED ${PLUGIN_SOURCES})

# Output names
set_target_properties(pgsamp PROPERTIES
    PREFIX ""                   # No "lib" prefix on Linux
    OUTPUT_NAME "pgsamp"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/output"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/output"
)

# ===========================================================================
# Include directories
# ===========================================================================
target_include_directories(pgsamp PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/libs/samp-sdk/sdk
)

# ===========================================================================
# Find libpq — MUST be 32-bit on all platforms
# ===========================================================================

# Priority 1: vcpkg unofficial-libpq (preferred when vcpkg toolchain is active)
set(_PQ_FOUND FALSE)

if(DEFINED VCPKG_TARGET_TRIPLET OR DEFINED CMAKE_TOOLCHAIN_FILE)
    find_package(unofficial-libpq CONFIG QUIET)
    if(unofficial-libpq_FOUND)
        message(STATUS "Using vcpkg libpq (${VCPKG_TARGET_TRIPLET})")
        target_link_libraries(pgsamp PRIVATE unofficial::libpq::libpq)
        set(_PQ_FOUND TRUE)
    else()
        # vcpkg installed but config not found — use known vcpkg paths directly
        if(DEFINED _VCPKG_INSTALLED_DIR AND DEFINED VCPKG_TARGET_TRIPLET)
            set(_VCPKG_PQ_INC "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include")
            set(_VCPKG_PQ_LIB "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libpq.lib")
            if(EXISTS "${_VCPKG_PQ_LIB}")
                message(STATUS "Using vcpkg libpq from ${_VCPKG_PQ_LIB}")
                target_include_directories(pgsamp PRIVATE "${_VCPKG_PQ_INC}")
                target_link_libraries(pgsamp PRIVATE "${_VCPKG_PQ_LIB}")
                set(_PQ_FOUND TRUE)
            endif()
        endif()
    endif()
endif()

# Priority 2: manually specified paths (override)
if(DEFINED LIBPQ_INCLUDE_DIR AND DEFINED LIBPQ_LIBRARY)
    message(STATUS "Using manually specified libpq: ${LIBPQ_LIBRARY}")
    target_include_directories(pgsamp PRIVATE "${LIBPQ_INCLUDE_DIR}")
    target_link_libraries(pgsamp PRIVATE "${LIBPQ_LIBRARY}")
    set(_PQ_FOUND TRUE)
endif()

# Priority 3: system FindPostgreSQL (last resort — may be x64 on Windows)
if(NOT _PQ_FOUND)
    find_package(PostgreSQL QUIET)
    if(PostgreSQL_FOUND)
        message(STATUS "Using system PostgreSQL: ${PostgreSQL_INCLUDE_DIRS}")
        message(WARNING
            "[pgsamp] Using system PostgreSQL. On Windows this is likely x64 and WILL fail to link.\n"
            "Use vcpkg x86-windows instead:\n"
            "  C:\\vcpkg\\vcpkg install libpq:x86-windows\n"
            "  cmake -B build -G \"Visual Studio 17 2022\" -A Win32\n"
            "        -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake\n"
            "        -DVCPKG_TARGET_TRIPLET=x86-windows")
        target_include_directories(pgsamp PRIVATE ${PostgreSQL_INCLUDE_DIRS})
        target_link_libraries(pgsamp PRIVATE ${PostgreSQL_LIBRARIES})
        set(_PQ_FOUND TRUE)
    endif()
endif()

if(NOT _PQ_FOUND)
    message(FATAL_ERROR
        "[pgsamp] libpq not found. Provide a 32-bit build via vcpkg:\n"
        "  C:\\vcpkg\\vcpkg install libpq:x86-windows\n"
        "  cmake -B build -G \"Visual Studio 17 2022\" -A Win32\n"
        "        -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake\n"
        "        -DVCPKG_TARGET_TRIPLET=x86-windows")
endif()

# ===========================================================================
# Platform-specific settings
# ===========================================================================
if(WIN32)
    # Windows: link against ws2_32 (required by libpq on Windows)
    target_link_libraries(pgsamp PRIVATE ws2_32 secur32)

    # Export definitions for MSVC
    if(MSVC)
        target_compile_options(pgsamp PRIVATE
            /W3
            /wd4996   # Suppress "deprecated" warnings for standard CRT functions
        )
        # Use a .def file to guarantee correct ordinal exports for SA-MP
        # (generated automatically by SDK macros, but explicit here for safety)
        set_target_properties(pgsamp PROPERTIES
            LINK_FLAGS "/SAFESEH:NO"
        )
    endif()

    # MinGW
    if(MINGW)
        target_link_options(pgsamp PRIVATE
            -static-libgcc
            -static-libstdc++
            -Wl,--no-undefined
        )
    endif()

else()
    # Linux
    target_compile_options(pgsamp PRIVATE
        -Wall
        -Wextra
        -Wno-unused-parameter
        -fvisibility=hidden
    )
    target_link_options(pgsamp PRIVATE
        -Wl,--no-undefined
    )

    # pthreads (used via std::thread)
    find_package(Threads REQUIRED)
    target_link_libraries(pgsamp PRIVATE Threads::Threads)
endif()

# ===========================================================================
# Compiler-specific optimisations
# ===========================================================================
if(NOT MSVC)
    target_compile_options(pgsamp PRIVATE -O2 -fPIC)
endif()

if(MSVC)
    target_compile_options(pgsamp PRIVATE /O2)
endif()

# ===========================================================================
# Install targets
# ===========================================================================
install(TARGETS pgsamp
    RUNTIME DESTINATION plugins   # Windows DLL
    LIBRARY DESTINATION plugins   # Linux SO
)
install(FILES postgres.inc DESTINATION pawno/include)

# ===========================================================================
# Summary
# ===========================================================================
message(STATUS "---------------------------------------------")
message(STATUS "  pgsamp PostgreSQL plugin v${PROJECT_VERSION}")
message(STATUS "  Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "  Install to : ${CMAKE_INSTALL_PREFIX}")
message(STATUS "---------------------------------------------")
