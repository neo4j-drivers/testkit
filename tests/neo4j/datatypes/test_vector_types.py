import math
import random
import struct

import nutkit.protocol as types
from tests.neo4j.datatypes._base import _TestTypesBase
from tests.neo4j.shared import requires_vector_support


class TestDataTypes(_TestTypesBase):

    required_features = (
        *_TestTypesBase.required_features,
        types.Feature.API_TYPE_VECTOR,
    )

    @requires_vector_support
    def test_should_echo_back_vector(self):
        vals = [
            types.CypherVector(
                "i8",
                b"\x01",
            ),
            types.CypherVector(
                "i8",
                b"\x01\x02\x03\x04",
            ),
            types.CypherVector(
                "i8",
                self._max_value_be_bytes(1, 4096),
            ),
            types.CypherVector(
                "i16",
                b"\x00\x01",
            ),
            types.CypherVector(
                "i16",
                b"\x00\x01\x00\x02",
            ),
            types.CypherVector(
                "i16",
                self._max_value_be_bytes(2, 4096),
            ),
            types.CypherVector(
                "i32",
                b"\x00\x00\x00\x01",
            ),
            types.CypherVector(
                "i32",
                b"\x00\x00\x00\x01\x00\x00\x00\x02",
            ),
            types.CypherVector(
                "i32",
                self._max_value_be_bytes(4, 4096),
            ),
            types.CypherVector(
                "i64",
                b"\x00\x00\x00\x00\x00\x00\x00\x01",
            ),
            types.CypherVector(
                "i64",
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x01"
                    b"\x00\x00\x00\x00\x00\x00\x00\x02"
                ),
            ),
            types.CypherVector(
                "i64",
                self._max_value_be_bytes(8, 4096),
            ),
            types.CypherVector(
                "f32",
                self._random_value_be_bytes(
                    4,
                    1,
                    validate=self._is_finite_f32,
                ),
            ),
            types.CypherVector(
                "f32",
                self._random_value_be_bytes(
                    4,
                    4096,
                    validate=self._is_finite_f32,
                ),
            ),
            types.CypherVector(
                "f32",
                (
                    # ±0.0
                    b"\x00\x00\x00\x00"
                    b"\x80\x00\x00\x00"
                    # smallest normal
                    b"\x00\x80\x00\x00"
                    b"\x80\x80\x00\x00"
                    # subnormal
                    b"\x00\x00\x00\x01"
                    b"\x80\x00\x00\x01"
                    # largest normal
                    b"\x7f\x7f\xff\xff"
                    b"\xff\x7f\xff\xff"
                )
            ),
            types.CypherVector(
                "f64",
                self._random_value_be_bytes(
                    8,
                    1,
                    validate=self._is_finite_f64,
                ),
            ),
            types.CypherVector(
                "f64",
                self._random_value_be_bytes(
                    8,
                    4096,
                    validate=self._is_finite_f64,
                ),
            ),
            types.CypherVector(
                "f64",
                (
                    # ±0.0
                    b"\x00\x00\x00\x00\x00\x00\x00\x00"
                    b"\x80\x00\x00\x00\x00\x00\x00\x00"
                    # smallest normal
                    b"\x00\x10\x00\x00\x00\x00\x00\x00"
                    b"\x80\x10\x00\x00\x00\x00\x00\x00"
                    # subnormal
                    b"\x00\x00\x00\x00\x00\x00\x00\x01"
                    b"\x80\x00\x00\x00\x00\x00\x00\x01"
                    # largest normal
                    b"\x7f\xef\xff\xff\xff\xff\xff\xff"
                    b"\xff\xef\xff\xff\xff\xff\xff\xff"
                )
            ),
        ]

        self._create_driver_and_session()
        for i, val in enumerate(vals):
            with self.subTest(x=i, len=len(val.data)):
                self._verify_can_echo(val)

    @requires_vector_support
    def test_should_fail_gracefully_on_vector_size_limits(self):
        vals = [
            types.CypherVector(
                "i8",
                b"",
            ),
            types.CypherVector(
                "i8",
                self._random_value_be_bytes(1, 4097),
            ),
            types.CypherVector(
                "i16",
                b"",
            ),
            types.CypherVector(
                "i16",
                self._random_value_be_bytes(2, 4097),
            ),
            types.CypherVector(
                "i32",
                b"",
            ),
            types.CypherVector(
                "i32",
                self._random_value_be_bytes(4, 4097),
            ),
            types.CypherVector(
                "i64",
                b"",
            ),
            types.CypherVector(
                "i64",
                self._random_value_be_bytes(8, 4097),
            ),
            types.CypherVector(
                "f32",
                b"",
            ),
            types.CypherVector(
                "f32",
                self._random_value_be_bytes(
                    4,
                    4097,
                    validate=self._is_finite_f32,
                ),
            ),
            types.CypherVector(
                "f64",
                b"",
            ),
            types.CypherVector(
                "f64",
                self._random_value_be_bytes(
                    8,
                    4097,
                    validate=self._is_finite_f64,
                ),
            ),
        ]

        self._create_driver_and_session()
        for val in vals:
            with self.subTest(dtype=val.dtype, size=len(val.data)):
                with self.assertRaises(types.DriverError) as e:
                    self._send_value(val)
                exc = e.exception
                self.assertEqual(exc.gql_status, "22NBE")
                self.assertIn("vector", exc.msg.lower())
                self.assertIn("dimension", exc.msg.lower())

    @requires_vector_support
    def test_should_fail_gracefully_on_invalid_float_values(self):
        vals = [
            *(
                types.CypherVector(
                    "f32",
                    bytes_,
                )
                for bytes_ in (
                    # ±inf
                    b"\x7F\x80\x00\x00",
                    b"\xFF\x80\x00\x00",
                    # NaN
                    b"\x7f\xc0\x00\x00",
                    b"\xff\xc0\x00\x00",
                    # signaling NaN
                    b"\x7f\xe0\x00\x00",
                    b"\xff\xe0\x00\x00",
                    # NaN payloads
                    b"\x7f\xdf\xff\xff",
                    b"\xff\xdf\xff\xff",
                    b"\x7f\xff\xff\xff",
                    b"\xff\xff\xff\xff",
                )
            ),
            *(
                types.CypherVector(
                    "f64",
                    bytes_,
                )
                for bytes_ in (
                    # ±inf
                    b"\x7f\xf0\x00\x00\x00\x00\x00\x00",
                    b"\xff\xf0\x00\x00\x00\x00\x00\x00",
                    # NaN
                    b"\x7f\xf8\x00\x00\x00\x00\x00\x00",
                    b"\xff\xf8\x00\x00\x00\x00\x00\x00",
                    # signaling NaN
                    b"\x7f\xfc\x00\x00\x00\x00\x00\x00",
                    b"\xff\xfc\x00\x00\x00\x00\x00\x00",
                    # NaN payloads
                    b"\x7f\xfb\xff\xff\xff\xff\xff\xff",
                    b"\xff\xfb\xff\xff\xff\xff\xff\xff",
                    b"\x7f\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        ]

        self._create_driver_and_session()
        for val in vals:
            with self.subTest(dtype=val.dtype, size=len(val.data)):
                with self.assertRaises(types.DriverError) as e:
                    self._send_value(val)
                exc = e.exception
                self.assertEqual(exc.gql_status, "22NBG")
                self.assertIn("vector", exc.msg.lower())
                self.assertIn("coordinate", exc.msg.lower())
                self.assertIn("finite", exc.msg.lower())

    @staticmethod
    def _max_value_be_bytes(size, count):
        def generator(count_):
            pack_format = {
                1: ">b",
                2: ">h",
                4: ">i",
                8: ">q",
            }[size]
            if count_ <= 0:
                return
            yield from struct.pack(pack_format, 0)
            count_ -= 1
            i = 0
            min_value = -(2 ** (size * 8 - 1))
            max_value = 2 ** (size * 8 - 1) - 1
            while True:
                if count_ <= 0:
                    return
                yield from struct.pack(pack_format, min_value + i)
                count_ -= 1
                if count_ == 0:
                    return
                yield from struct.pack(pack_format, max_value - i)
                count_ -= 1
                i += 1
                i %= 2 ** (size * 8)

        return bytes(generator(count))

    @staticmethod
    def _random_value_be_bytes(size, count, validate=None):
        def generator(count_):
            pack_format = {
                1: ">B",
                2: ">H",
                4: ">I",
                8: ">Q",
            }[size]
            while count_ > 0:
                bytes_ = struct.pack(
                    pack_format, random.randint(0, 2 ** (size * 8) - 1)
                )
                if validate is None or validate(bytes_):
                    yield from bytes_
                    count_ -= 1

        return bytes(generator(count))

    @staticmethod
    def _is_finite_f32(be_bytes):
        value = struct.unpack(">f", be_bytes)[0]
        return math.isfinite(value)

    @staticmethod
    def _is_finite_f64(be_bytes):
        value = struct.unpack(">d", be_bytes)[0]
        return math.isfinite(value)
