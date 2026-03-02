cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

project(ErlangPythonNIF C)

# CMake policies
if(POLICY CMP0028)
  cmake_policy(SET CMP0028 NEW)
endif()
if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake")

# Add standard package manager paths for macOS (MacPorts and Homebrew)
if(APPLE)
    # MacPorts
    if(EXISTS "/opt/local")
        list(APPEND CMAKE_PREFIX_PATH "/opt/local")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/local/include")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/local/lib")
    endif()
    # Homebrew on Apple Silicon
    if(EXISTS "/opt/homebrew")
        list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/homebrew/include")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/homebrew/lib")
    endif()
    # Homebrew on Intel (default location)
    if(EXISTS "/usr/local/include")
        list(APPEND CMAKE_PREFIX_PATH "/usr/local")
        list(APPEND CMAKE_INCLUDE_PATH "/usr/local/include")
        list(APPEND CMAKE_LIBRARY_PATH "/usr/local/lib")
    endif()
endif()

# Output directory
set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})
file(MAKE_DIRECTORY ${priv_dir})

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Performance build option (for maximum optimization)
option(PERF_BUILD "Enable aggressive performance optimizations (-O3, LTO, native arch)" OFF)

# ASGI profiling option (for internal timing analysis)
option(ASGI_PROFILING "Enable ASGI internal profiling" OFF)
if(ASGI_PROFILING)
    message(STATUS "ASGI profiling enabled - timing instrumentation active")
    add_definitions(-DASGI_PROFILING)
endif()

if(PERF_BUILD)
    message(STATUS "Performance build enabled - using aggressive optimizations")
    # Override compiler flags for maximum performance
    set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")
    # Enable Link-Time Optimization
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# Find Erlang
include(FindErlang)
include_directories(${ERLANG_ERTS_INCLUDE_PATH})

# Find Python using CMake's built-in FindPython3
#
# To specify a particular Python installation, set PYTHON_CONFIG env variable:
#   PYTHON_CONFIG=/opt/local/bin/python3.14-config cmake ...
#
# CMake will use its default search order otherwise.

if(DEFINED ENV{PYTHON_CONFIG})
    # Extract prefix from python-config for hinting
    execute_process(
        COMMAND $ENV{PYTHON_CONFIG} --prefix
        OUTPUT_VARIABLE Python3_ROOT_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    set(Python3_FIND_STRATEGY LOCATION)
endif()

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

message(STATUS "Python3 executable: ${Python3_EXECUTABLE}")
message(STATUS "Python3 version: ${Python3_VERSION}")
message(STATUS "Python3 include dirs: ${Python3_INCLUDE_DIRS}")
message(STATUS "Python3 libraries: ${Python3_LIBRARIES}")
message(STATUS "Python3 library: ${Python3_LIBRARY}")

# Detect Python features for worker pool optimization
# We use both version checks and compile tests to verify actual API availability

include(CheckCSourceCompiles)

# First check Python version - subinterpreters with OWN_GIL require Python 3.12+
if(Python3_VERSION VERSION_GREATER_EQUAL "3.12")
    message(STATUS "Python ${Python3_VERSION} >= 3.12, checking subinterpreter API...")

    # Save and set required variables for compile test
    set(CMAKE_REQUIRED_INCLUDES ${Python3_INCLUDE_DIRS})
    set(CMAKE_REQUIRED_LIBRARIES Python3::Python)

    # Clear any cached result to ensure fresh detection
    unset(HAVE_SUBINTERPRETERS CACHE)

    # Check for subinterpreter API with per-interpreter GIL (PEP 684, Python 3.12+)
    # This verifies PyInterpreterConfig and PyInterpreterConfig_OWN_GIL are available
    check_c_source_compiles("
#define PY_SSIZE_T_CLEAN
#include <Python.h>
int main(void) {
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,
    };
    (void)config;
    return 0;
}
" HAVE_SUBINTERPRETERS)

    if(HAVE_SUBINTERPRETERS)
        message(STATUS "Subinterpreter API detected (PyInterpreterConfig_OWN_GIL available)")
    else()
        message(STATUS "Subinterpreter API compile test failed, using shared GIL fallback")
    endif()
else()
    message(STATUS "Python ${Python3_VERSION} < 3.12, subinterpreter API not available")
    set(HAVE_SUBINTERPRETERS FALSE)
endif()

# Check for free-threaded Python (Python 3.13+ with --disable-gil / nogil build)
# Free-threaded builds have Py_GIL_DISABLED defined in sysconfig
execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import sysconfig; print('yes' if sysconfig.get_config_var('Py_GIL_DISABLED') else 'no')"
    OUTPUT_VARIABLE Python3_FREE_THREADED
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(Python3_FREE_THREADED STREQUAL "yes")
    set(HAVE_FREE_THREADED TRUE)
    message(STATUS "Free-threaded Python detected: GIL disabled at runtime")
else()
    set(HAVE_FREE_THREADED FALSE)
endif()

# Create the NIF shared library
add_library(py_nif MODULE py_nif.c)

# Pass Python library path to NIF for dlopen
# Python3_LIBRARY is the full path to the Python shared library
if(Python3_LIBRARY)
    target_compile_definitions(py_nif PRIVATE PYTHON_LIBRARY_PATH="${Python3_LIBRARY}")
    message(STATUS "Using Python library path for dlopen: ${Python3_LIBRARY}")
elseif(Python3_LIBRARIES)
    # Fallback to first library in the list
    list(GET Python3_LIBRARIES 0 Python3_FIRST_LIB)
    target_compile_definitions(py_nif PRIVATE PYTHON_LIBRARY_PATH="${Python3_FIRST_LIB}")
    message(STATUS "Using Python library path for dlopen: ${Python3_FIRST_LIB}")
endif()

# Add Python feature compile definitions for worker pool optimization
if(HAVE_SUBINTERPRETERS)
    target_compile_definitions(py_nif PRIVATE HAVE_SUBINTERPRETERS=1)
endif()
if(HAVE_FREE_THREADED)
    target_compile_definitions(py_nif PRIVATE HAVE_FREE_THREADED=1)
endif()

# Set output name
set_target_properties(py_nif PROPERTIES
    PREFIX ""
    OUTPUT_NAME "py_nif"
)

# Include directories
target_include_directories(py_nif PRIVATE
    ${ERLANG_ERTS_INCLUDE_PATH}
    ${Python3_INCLUDE_DIRS}
)

# Compiler flags
if(PERF_BUILD)
    # Performance build: aggressive optimizations
    target_compile_options(py_nif PRIVATE
        -O3
        -Wall
        -fPIC
        -march=native
        -ffast-math
        -funroll-loops
    )
else()
    # Standard build
    target_compile_options(py_nif PRIVATE
        -O2
        -Wall
        -fPIC
    )
endif()

# Platform-specific settings
if(APPLE)
    target_link_options(py_nif PRIVATE
        -undefined dynamic_lookup
        -flat_namespace
    )
    # Add CoreFoundation framework
    target_link_libraries(py_nif PRIVATE "-framework CoreFoundation")
elseif(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR
       CMAKE_SYSTEM_NAME STREQUAL "NetBSD" OR
       CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    # BSD systems
    target_link_options(py_nif PRIVATE
        -Wl,--export-dynamic
    )
    # No need to link libdl on BSD - dlopen is in libc
elseif(UNIX)
    # Linux
    target_link_options(py_nif PRIVATE
        -Wl,--export-dynamic
    )
    # dlopen for loading libpython with RTLD_GLOBAL
    target_link_libraries(py_nif PRIVATE dl)
endif()

# Link Python
target_link_libraries(py_nif PRIVATE Python3::Python)

# Threads
find_package(Threads REQUIRED)
target_link_libraries(py_nif PRIVATE Threads::Threads)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Output directory: ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
