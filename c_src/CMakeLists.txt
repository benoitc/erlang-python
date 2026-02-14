cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

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

# Find Erlang
include(FindErlang)
include_directories(${ERLANG_ERTS_INCLUDE_PATH})

# Find Python - use the PYTHON_CONFIG env variable if set
if(DEFINED ENV{PYTHON_CONFIG})
    set(Python3_CONFIG "$ENV{PYTHON_CONFIG}")
    execute_process(
        COMMAND ${Python3_CONFIG} --prefix
        OUTPUT_VARIABLE Python3_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
        COMMAND ${Python3_CONFIG} --includes
        OUTPUT_VARIABLE Python3_INCLUDE_FLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
        COMMAND ${Python3_CONFIG} --ldflags --embed
        OUTPUT_VARIABLE Python3_LINK_FLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE Python3_EMBED_RESULT
    )
    if(NOT Python3_EMBED_RESULT EQUAL 0)
        execute_process(
            COMMAND ${Python3_CONFIG} --ldflags
            OUTPUT_VARIABLE Python3_LINK_FLAGS
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    endif()

    # Extract include directories from flags
    string(REGEX MATCHALL "-I[^ ]+" Python3_INCLUDE_DIRS_LIST "${Python3_INCLUDE_FLAGS}")
    string(REGEX REPLACE "-I" "" Python3_INCLUDE_DIRS_LIST "${Python3_INCLUDE_DIRS_LIST}")

    message(STATUS "Using Python config: ${Python3_CONFIG}")
    message(STATUS "Python includes: ${Python3_INCLUDE_FLAGS}")
    message(STATUS "Python link flags: ${Python3_LINK_FLAGS}")
else()
    # Use CMake's FindPython3
    find_package(Python3 REQUIRED COMPONENTS Development)
    set(Python3_INCLUDE_DIRS_LIST ${Python3_INCLUDE_DIRS})
    set(Python3_LINK_FLAGS "${Python3_LIBRARIES}")
endif()

# Create the NIF shared library
add_library(py_nif MODULE py_nif.c)

# Set output name
set_target_properties(py_nif PROPERTIES
    PREFIX ""
    OUTPUT_NAME "py_nif"
)

# Include directories
target_include_directories(py_nif PRIVATE
    ${ERLANG_ERTS_INCLUDE_PATH}
    ${Python3_INCLUDE_DIRS_LIST}
)

# Compiler flags
target_compile_options(py_nif PRIVATE
    -O2
    -Wall
    -fPIC
)

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
if(DEFINED ENV{PYTHON_CONFIG})
    # Parse the link flags and add them properly
    separate_arguments(Python3_LINK_FLAGS_LIST UNIX_COMMAND "${Python3_LINK_FLAGS}")
    target_link_libraries(py_nif PRIVATE ${Python3_LINK_FLAGS_LIST})
else()
    target_link_libraries(py_nif PRIVATE Python3::Python)
endif()

# Threads
find_package(Threads REQUIRED)
target_link_libraries(py_nif PRIVATE Threads::Threads)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Output directory: ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
