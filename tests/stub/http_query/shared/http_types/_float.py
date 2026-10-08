from __future__ import annotations

import math
import re
import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Float",)

FLOAT_RE = re.compile(
    r"[+-]?Infinity|"
    r"[+-]?NaN|"
    r"[+-]?(?:\d+(\.\d*)?|\d*\.\d+)(?:[eE][+-]?\d+)?"
)


class Float(HttpType):
    """
    Float value.

    This float type compares nan == nan as true intentionally.
    """

    value: float

    def __init__(self, value: float) -> None:
        self.value = value

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Float", self._str()).serialize()

    def _str(self) -> str:
        str_value = str(self.value)
        str_value = str_value.replace("inf", "Infinity").replace("nan", "NaN")
        return str_value

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "Float":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        match = FLOAT_RE.fullmatch(v)
        if match is None:
            value_dict.invalid_value(f"didn't match float regex: {v!r}")
            return None
        try:
            float_value = float(v)
        except ValueError as e:
            value_dict.invalid_value(f"invalid float string {v!r}: {e!r}")
            return None

        return cls(float_value)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if isinstance(value, float):
            return cls(value)
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherFloat):
            v = value.value
            if isinstance(v, float):
                return cls(v)
            if isinstance(v, str):
                return cls(float(v))
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            other_val = t.cast(Float, other).value
            if isinstance(self.value, float) and isinstance(other_val, float):
                if math.isnan(self.value) and math.isnan(other_val):
                    return True
            return self.value == other_val
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._str()}>"
