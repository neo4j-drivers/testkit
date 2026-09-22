from __future__ import annotations

import enum

__all__ = ("ProtocolVersion",)


class ProtocolVersion(enum.Enum):
    V1_0 = "1.0"
    V1_1 = "1.1"
