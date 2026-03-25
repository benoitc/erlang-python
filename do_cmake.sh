#!/bin/sh -x

mkdir -p _build/cmake
cd _build/cmake

if type cmake3 > /dev/null 2>&1 ; then
    CMAKE=cmake3
else
    CMAKE=cmake
fi

# Support CMAKE_OPTIONS env variable for additional cmake flags
${CMAKE} ${CMAKE_OPTIONS:-} "$@" ../../c_src || exit 1

echo done.
