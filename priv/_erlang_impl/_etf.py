# Copyright 2026 Benoit Chesneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Erlang external term format (ETF) codec, pure Python.

Used by the isolated child process, where no NIF is available. The type
mapping mirrors c_src/py_convert.c so code behaves the same in `worker`,
`owngil` and `isolated` mode:

    Erlang -> Python                     Python -> Erlang
    true / false      -> True / False    None            -> none
    none/nil/undefined-> None            bool            -> true / false
    other atom        -> str             int             -> integer (any size)
    integer           -> int             float           -> float (nan/inf -> atoms)
    float             -> float           str, bytes      -> binary
    binary (utf-8)    -> str             list            -> list
    binary (other)    -> bytes           tuple           -> tuple
    {bytes, Bin}      -> bytes           dict            -> map
    list / string     -> list            Atom            -> atom
    tuple             -> tuple           Pid / Ref / Port-> pid / ref / port
    map               -> dict            numpy ndarray   -> list (tolist)
    pid / ref / port  -> Pid / Ref / Port other object   -> binary(str(obj))

Pids, refs and ports are opaque: the decoder keeps their raw ETF bytes and
the encoder emits them unchanged, so they round-trip exactly.
"""

import math
import struct

__all__ = [
    'Atom', 'Pid', 'Ref', 'Port',
    'encode', 'decode', 'DecodeError', 'register_encoder',
]

# (predicate, to_term) pairs consulted before the generic fallback
_encoders = []


def register_encoder(predicate, to_term):
    """Encode objects matching `predicate` as the term `to_term(obj)` returns."""
    _encoders.append((predicate, to_term))

VERSION = 131

# Tags
NEW_FLOAT_EXT = 70
BIT_BINARY_EXT = 77
NEW_PID_EXT = 88
NEW_PORT_EXT = 89
NEWER_REFERENCE_EXT = 90
SMALL_INTEGER_EXT = 97
INTEGER_EXT = 98
FLOAT_EXT = 99
ATOM_EXT = 100
REFERENCE_EXT = 101
PORT_EXT = 102
PID_EXT = 103
SMALL_TUPLE_EXT = 104
LARGE_TUPLE_EXT = 105
NIL_EXT = 106
STRING_EXT = 107
LIST_EXT = 108
BINARY_EXT = 109
SMALL_BIG_EXT = 110
LARGE_BIG_EXT = 111
NEW_REFERENCE_EXT = 114
SMALL_ATOM_EXT = 115
MAP_EXT = 116
ATOM_UTF8_EXT = 118
SMALL_ATOM_UTF8_EXT = 119
V4_PORT_EXT = 120


class DecodeError(ValueError):
    """Raised on malformed external term format data."""


class Atom(str):
    """An Erlang atom. A str subclass so `Atom('ok') == 'ok'`, and so code
    written for the embedded erlang.Atom keeps working."""

    __slots__ = ()

    def __new__(cls, name):
        if isinstance(name, bytes):
            name = name.decode('utf-8')
        if not isinstance(name, str):
            raise TypeError('atom name must be str')
        return str.__new__(cls, name)

    def __repr__(self):
        return 'erlang.Atom(%r)' % str.__str__(self)


class _Opaque:
    """Base for pid/ref/port: value semantics over the raw ETF bytes."""

    __slots__ = ('_raw',)

    def __init__(self, raw):
        if not isinstance(raw, (bytes, bytearray)):
            raise TypeError('%s wants raw ETF bytes' % type(self).__name__)
        self._raw = bytes(raw)

    def __eq__(self, other):
        return type(other) is type(self) and other._raw == self._raw

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((type(self).__name__, self._raw))

    def __repr__(self):
        return '<erlang.%s %s>' % (type(self).__name__, self._raw.hex())

    @property
    def raw(self):
        return self._raw


class Pid(_Opaque):
    __slots__ = ()

    def __repr__(self):
        try:
            node, ident, serial, _ = _decode_pid_fields(self._raw)
            return '<erlang.Pid <0.%d.%d>@%s>' % (ident, serial, node)
        except Exception:
            return _Opaque.__repr__(self)


class Ref(_Opaque):
    __slots__ = ()


class Port(_Opaque):
    __slots__ = ()


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------

_pack_u8 = struct.Struct('>B').pack
_pack_u16 = struct.Struct('>H').pack
_pack_u32 = struct.Struct('>I').pack
_pack_i32 = struct.Struct('>i').pack
_pack_f64 = struct.Struct('>d').pack


def encode(obj):
    """Encode a Python object as a complete ETF binary (with version byte)."""
    out = bytearray([VERSION])
    _encode(obj, out)
    return bytes(out)


def _encode_atom(name, out):
    data = name.encode('utf-8')
    n = len(data)
    if n > 255:
        raise ValueError('atom too long: %d bytes' % n)
    if n < 256:
        out += _pack_u8(SMALL_ATOM_UTF8_EXT)
        out += _pack_u8(n)
    out += data


def _encode(obj, out):
    if obj is None:
        _encode_atom('none', out)
    elif obj is True:
        _encode_atom('true', out)
    elif obj is False:
        _encode_atom('false', out)
    elif isinstance(obj, Atom):
        _encode_atom(str.__str__(obj), out)
    elif isinstance(obj, int):
        _encode_int(obj, out)
    elif isinstance(obj, float):
        if math.isnan(obj):
            _encode_atom('nan', out)
        elif math.isinf(obj):
            _encode_atom('infinity' if obj > 0 else 'neg_infinity', out)
        else:
            out += _pack_u8(NEW_FLOAT_EXT)
            out += _pack_f64(obj)
    elif isinstance(obj, str):
        data = obj.encode('utf-8', 'surrogatepass')
        out += _pack_u8(BINARY_EXT)
        out += _pack_u32(len(data))
        out += data
    elif isinstance(obj, (bytes, bytearray, memoryview)):
        data = bytes(obj)
        out += _pack_u8(BINARY_EXT)
        out += _pack_u32(len(data))
        out += data
    elif isinstance(obj, _Opaque):
        out += obj._raw
    elif isinstance(obj, tuple):
        n = len(obj)
        if n < 256:
            out += _pack_u8(SMALL_TUPLE_EXT)
            out += _pack_u8(n)
        else:
            out += _pack_u8(LARGE_TUPLE_EXT)
            out += _pack_u32(n)
        for item in obj:
            _encode(item, out)
    elif isinstance(obj, list):
        n = len(obj)
        if n == 0:
            out += _pack_u8(NIL_EXT)
        else:
            out += _pack_u8(LIST_EXT)
            out += _pack_u32(n)
            for item in obj:
                _encode(item, out)
            out += _pack_u8(NIL_EXT)
    elif isinstance(obj, dict):
        out += _pack_u8(MAP_EXT)
        out += _pack_u32(len(obj))
        for k, v in obj.items():
            _encode(k, out)
            _encode(v, out)
    elif _is_ndarray(obj):
        _encode(obj.tolist(), out)
    elif isinstance(obj, (set, frozenset)):
        _encode(list(obj), out)
    else:
        for predicate, to_term in _encoders:
            if predicate(obj):
                _encode(to_term(obj), out)
                return
        # Same fallback as py_to_term: the string representation as a binary
        _encode(str(obj), out)


def _encode_int(value, out):
    if 0 <= value <= 255:
        out += _pack_u8(SMALL_INTEGER_EXT)
        out += _pack_u8(value)
    elif -2147483648 <= value <= 2147483647:
        out += _pack_u8(INTEGER_EXT)
        out += _pack_i32(value)
    else:
        sign = 1 if value < 0 else 0
        mag = -value if sign else value
        n = (mag.bit_length() + 7) // 8
        digits = mag.to_bytes(n, 'little')
        if n < 256:
            out += _pack_u8(SMALL_BIG_EXT)
            out += _pack_u8(n)
        else:
            out += _pack_u8(LARGE_BIG_EXT)
            out += _pack_u32(n)
        out += _pack_u8(sign)
        out += digits


def _is_ndarray(obj):
    t = type(obj)
    if t.__module__ == 'numpy' and t.__name__ == 'ndarray':
        return True
    return hasattr(obj, 'tolist') and hasattr(obj, 'ndim')


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------

_unpack_u16 = struct.Struct('>H').unpack_from
_unpack_u32 = struct.Struct('>I').unpack_from
_unpack_i32 = struct.Struct('>i').unpack_from
_unpack_f64 = struct.Struct('>d').unpack_from

_ATOM_TRUE = 'true'
_ATOM_FALSE = 'false'
_NONE_ATOMS = frozenset(('none', 'nil', 'undefined'))


def decode(data):
    """Decode a complete ETF binary (with version byte) to a Python object."""
    if not data or data[0] != VERSION:
        raise DecodeError('bad ETF version byte')
    value, pos = _decode(data, 1)
    if pos != len(data):
        raise DecodeError('trailing bytes after term')
    return value


def _atom_value(name):
    """Map an atom to its Python value the way term_to_py does."""
    if name == _ATOM_TRUE:
        return True
    if name == _ATOM_FALSE:
        return False
    if name in _NONE_ATOMS:
        return None
    return name


def _decode(data, pos):
    try:
        tag = data[pos]
    except IndexError:
        raise DecodeError('truncated term') from None
    pos += 1

    if tag == SMALL_INTEGER_EXT:
        return data[pos], pos + 1
    if tag == INTEGER_EXT:
        return _unpack_i32(data, pos)[0], pos + 4
    if tag == BINARY_EXT:
        (n,) = _unpack_u32(data, pos)
        pos += 4
        raw = bytes(data[pos:pos + n])
        if len(raw) != n:
            raise DecodeError('truncated binary')
        try:
            return raw.decode('utf-8'), pos + n
        except UnicodeDecodeError:
            return raw, pos + n
    if tag == SMALL_ATOM_UTF8_EXT:
        n = data[pos]
        pos += 1
        return _atom_value(bytes(data[pos:pos + n]).decode('utf-8')), pos + n
    if tag == ATOM_UTF8_EXT:
        (n,) = _unpack_u16(data, pos)
        pos += 2
        return _atom_value(bytes(data[pos:pos + n]).decode('utf-8')), pos + n
    if tag == ATOM_EXT:
        (n,) = _unpack_u16(data, pos)
        pos += 2
        return _atom_value(bytes(data[pos:pos + n]).decode('latin-1')), pos + n
    if tag == SMALL_ATOM_EXT:
        n = data[pos]
        pos += 1
        return _atom_value(bytes(data[pos:pos + n]).decode('latin-1')), pos + n
    if tag == SMALL_TUPLE_EXT or tag == LARGE_TUPLE_EXT:
        if tag == SMALL_TUPLE_EXT:
            n = data[pos]
            pos += 1
        else:
            (n,) = _unpack_u32(data, pos)
            pos += 4
        items = []
        for _ in range(n):
            item, pos = _decode(data, pos)
            items.append(item)
        # {bytes, Bin}: explicit bytes, as in term_to_py
        if n == 2 and items[0] == 'bytes' and isinstance(items[1], (str, bytes)):
            b = items[1]
            if isinstance(b, str):
                b = b.encode('utf-8', 'surrogatepass')
            return b, pos
        return tuple(items), pos
    if tag == NIL_EXT:
        return [], pos
    if tag == STRING_EXT:
        (n,) = _unpack_u16(data, pos)
        pos += 2
        return list(data[pos:pos + n]), pos + n
    if tag == LIST_EXT:
        (n,) = _unpack_u32(data, pos)
        pos += 4
        items = []
        for _ in range(n):
            item, pos = _decode(data, pos)
            items.append(item)
        # Tail: NIL for a proper list. An improper tail is kept as a final
        # element, the same as enif_get_list_length failing is not an option
        # here; this never happens for term_to_binary of proper lists.
        if data[pos] == NIL_EXT:
            pos += 1
        else:
            tail, pos = _decode(data, pos)
            items.append(tail)
        return items, pos
    if tag == MAP_EXT:
        (n,) = _unpack_u32(data, pos)
        pos += 4
        result = {}
        for _ in range(n):
            k, pos = _decode(data, pos)
            v, pos = _decode(data, pos)
            result[_hashable(k)] = v
        return result, pos
    if tag == NEW_FLOAT_EXT:
        return _unpack_f64(data, pos)[0], pos + 8
    if tag == FLOAT_EXT:
        text = bytes(data[pos:pos + 31]).split(b'\x00', 1)[0]
        return float(text), pos + 31
    if tag == SMALL_BIG_EXT or tag == LARGE_BIG_EXT:
        if tag == SMALL_BIG_EXT:
            n = data[pos]
            pos += 1
        else:
            (n,) = _unpack_u32(data, pos)
            pos += 4
        sign = data[pos]
        pos += 1
        value = int.from_bytes(bytes(data[pos:pos + n]), 'little')
        return (-value if sign else value), pos + n
    if tag in (NEW_PID_EXT, PID_EXT):
        start = pos - 1
        _, pos = _decode_atom_raw(data, pos)   # node
        pos += 8                              # id, serial
        pos += 4 if tag == NEW_PID_EXT else 1  # creation
        return Pid(data[start:pos]), pos
    if tag in (NEWER_REFERENCE_EXT, NEW_REFERENCE_EXT):
        start = pos - 1
        (n,) = _unpack_u16(data, pos)
        pos += 2
        _, pos = _decode_atom_raw(data, pos)
        pos += 4 if tag == NEWER_REFERENCE_EXT else 1
        pos += 4 * n
        return Ref(data[start:pos]), pos
    if tag == REFERENCE_EXT:
        start = pos - 1
        _, pos = _decode_atom_raw(data, pos)
        pos += 5
        return Ref(data[start:pos]), pos
    if tag in (NEW_PORT_EXT, PORT_EXT, V4_PORT_EXT):
        start = pos - 1
        _, pos = _decode_atom_raw(data, pos)
        pos += 8 if tag == V4_PORT_EXT else 4
        pos += 1 if tag == PORT_EXT else 4
        return Port(data[start:pos]), pos
    if tag == BIT_BINARY_EXT:
        (n,) = _unpack_u32(data, pos)
        pos += 4
        bits = data[pos]
        pos += 1
        raw = bytes(data[pos:pos + n])
        return (raw, bits), pos + n
    raise DecodeError('unsupported ETF tag %d' % tag)


def _decode_atom_raw(data, pos):
    """Decode an atom (any encoding) returning its name; used inside
    pid/ref/port where the value is not mapped."""
    tag = data[pos]
    pos += 1
    if tag == SMALL_ATOM_UTF8_EXT or tag == SMALL_ATOM_EXT:
        n = data[pos]
        pos += 1
    elif tag == ATOM_UTF8_EXT or tag == ATOM_EXT:
        (n,) = _unpack_u16(data, pos)
        pos += 2
    else:
        raise DecodeError('expected atom, got tag %d' % tag)
    return bytes(data[pos:pos + n]).decode('utf-8', 'replace'), pos + n


def _decode_pid_fields(raw):
    tag = raw[0]
    node, pos = _decode_atom_raw(raw, 1)
    (ident,) = _unpack_u32(raw, pos)
    (serial,) = _unpack_u32(raw, pos + 4)
    pos += 8
    if tag == NEW_PID_EXT:
        (creation,) = _unpack_u32(raw, pos)
    else:
        creation = raw[pos]
    return node, ident, serial, creation


def _hashable(key):
    """Map keys must be hashable; lists (Erlang lists/strings) become tuples."""
    if isinstance(key, list):
        return tuple(_hashable(k) for k in key)
    if isinstance(key, dict):
        return tuple(sorted((_hashable(k), _hashable(v)) for k, v in key.items()))
    return key
