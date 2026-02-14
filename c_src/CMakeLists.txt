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

# Find Erlang
include(FindErlang)
include_directories(${ERLANG_ERTS_INCLUDE_PATH})

# Find Python using CMake's built-in FindPython3
# Use PYTHON_CONFIG env variable to hint which Python to use
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
target_link_libraries(py_nif PRIVATE Python3::Python)

# Threads
find_package(Threads REQUIRED)
target_link_libraries(py_nif PRIVATE Threads::Threads)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Output directory: ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
