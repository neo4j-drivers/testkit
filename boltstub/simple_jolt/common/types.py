import abc
import re

from .errors import JOLTValueError


class JoltType:
    pass


class JoltWildcard(JoltType):
    """
    This is a stub-server specific JOLT type that marks a match-all object.

    e.g. `{"Z": "*"}` represents any integer.
    """

    def __init__(self, types):
        self.types = types


class _JoltParsedType(JoltType, abc.ABC):
    # to be overridden in subclasses (this re never matches)
    _parse_re = re.compile(r"^(?= )$")

    @abc.abstractmethod
    def __init__(self, value: str):
        match = self._parse_re.match(value)
        if not match:
            raise JOLTValueError(
                "{} didn't match the types format: {}".format(
                    value, self._parse_re
                )
            )
        self._str = value
        self._groups = match.groups()

    def __str__(self):
        return self._str


__all__ = [
    JoltType,
    JoltWildcard,
]
