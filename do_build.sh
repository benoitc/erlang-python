#!/bin/sh -x

cd _build/cmake

if type cmake3 > /dev/null 2>&1 ; then
    CMAKE=cmake3
else
    CMAKE=cmake
fi

case "$@" in
    *-j*)
        ${CMAKE} --build . -- "$@" || exit 1
        ;;
    *)
        CORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null)

        if [ "x$CORES" = "x" ]; then
            PAR=""
        else
            PAR="-- -j $CORES"
        fi
        ${CMAKE} --build . $PAR $@ || exit 1
        ;;
esac

echo done.
