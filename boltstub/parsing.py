# Copyright (c) "Neo4j,"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import abc
import json
import math
import re
import sys
import threading
import warnings
from collections import OrderedDict
from copy import deepcopy
from os import path
from textwrap import wrap
from time import sleep
from typing import (
    List,
    Optional,
)

import lark

from .bolt_protocol import (
    get_bolt_protocol,
    verify_script_messages,
)
from .errors import (
    BoltMissingVersionError,
    BoltUnknownMessageError,
    BoltUnknownVersionError,
    ServerExit,
)
from .packstream import Structure
from .simple_jolt.common.types import (
    JoltType,
    JoltWildcard,
)
from .util import EvalContext


def load_parser():
    grammar_path = path.join(path.dirname(__file__), "grammar.lark")
    with open(grammar_path, "r", encoding="utf-8") as fd:
        return lark.Lark(
            fd, propagate_positions=True  # , ambiguity="explicit"
        )


parser = load_parser()


class CopyableRLock:
    def __init__(self):
        self._l = threading.RLock()

    def __deepcopy__(self, memodict=None):
        return CopyableRLock()

    def acquire(self, blocking=True, timeout=-1):
        return self._l.acquire(blocking=blocking, timeout=timeout)

    def release(self):
        return self._l.release()

    def __enter__(self):
        return self._l.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._l.__exit__(exc_type, exc_val, exc_tb)


class LineError(lark.GrammarError):
    def __init__(self, line, *args, **kwargs):
        assert isinstance(line, Line)
        self.line = line
        if args and isinstance(args[0], str):
            args = (args[0] + ": {}".format(line),) + args[1:]
        else:
            args = ("{}".format(line),) + args
        super().__init__(*args, **kwargs)

    def __repr__(self):
        str(self.args)


class Line(str, abc.ABC):
    def __new__(cls, line_number: int, raw_line, content: str):
        obj = super(Line, cls).__new__(cls, raw_line)
        obj.line_number = line_number
        obj.content = content
        return obj

    def __str__(self):
        return "({:3}) {}".format(self.line_number,
                                  super(Line, self).__str__())

    def __repr__(self):
        return "<{}>{}".format(self.__class__.__name__, self.__str__())

    def __getnewargs__(self):
        return self.line_number, super(Line, self).__str__(), self.content

    @abc.abstractmethod
    def canonical(self):
        pass

    def parse_jolt(self, jolt_package):
        pass


class BangLine(Line):
    TYPE_AUTO = "auto"
    TYPE_BOLT = "bolt"
    TYPE_RESTART = "restart"
    TYPE_CONCURRENT = "concurrent"
    TYPE_HANDSHAKE = "handshake"
    TYPE_HANDSHAKE_DELAY = "handshake_delay"
    TYPE_PYTHON = "python"

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        if re.match(r"^AUTO\s", obj.content):
            obj._type = BangLine.TYPE_AUTO
            obj._arg = obj.content[5:].strip()
        elif re.match(r"^BOLT\s", obj.content):
            obj._type = BangLine.TYPE_BOLT
            raw_arg = obj.content[5:].strip()
            try:
                obj._arg = tuple(map(int, raw_arg.split(".")))
            except ValueError:
                raise LineError(
                    obj,
                    "invalid argument for bolt version, must be semantic "
                    "version (e.g. 'BOLT 4.2')"
                )
        elif re.match(r"^ALLOW\s+RESTART$", obj.content):
            obj._type = BangLine.TYPE_RESTART
            obj._arg = None
        elif re.match(r"^ALLOW\s+CONCURRENT", obj.content):
            obj._type = BangLine.TYPE_CONCURRENT
            obj._arg = None
        elif re.match(r"^HANDSHAKE\s", obj.content):
            obj._type = BangLine.TYPE_HANDSHAKE
            arg = re.sub(r"\s", "", obj.content[10:])
            if not re.match(r"^([0-9a-fA-F]{2})+$", arg):
                raise LineError(
                    obj,
                    "invalid argument for handshake, must be list of 2-digit "
                    "hex encoded bytes, whitespace is ignored (e.g. "
                    "'HANDSHAKE 00 FF 02 04 F0'"
                )
            obj._arg = bytearray(int(b, 16) for b in wrap(arg, 2))
        elif re.match(r"^HANDSHAKE_DELAY\s", obj.content):
            obj._type = BangLine.TYPE_HANDSHAKE_DELAY
            raw_arg = obj.content[16:].strip()
            try:
                obj._arg = float(raw_arg)
            except ValueError:
                raise LineError(
                    obj,
                    "invalid argument for handshake delay, must be a number "
                    "number (e.g. 'HANDSHAKE_DELAY 0.5')"
                )
            if obj._arg < 0:
                raise LineError(
                    obj,
                    "invalid argument for handshake delay, must be a positive "
                    "number (e.g. 'HANDSHAKE_DELAY 0.5')"
                )
        elif re.match(r"^PY\s", obj.content):
            obj._type = BangLine.TYPE_PYTHON
            obj._arg = obj.content[3:].strip()
        else:
            raise LineError(obj, 'unsupported Bang line: "{}"'.format(obj))
        return obj

    def canonical(self):
        return "!: {}".format(self.content)

    def update_context(self, ctx: "ScriptContext"):
        if self._type == BangLine.TYPE_AUTO:
            if self._arg in ctx.auto:
                warnings.warn(  # noqa: B028
                    f'Specified AUTO for "{self._arg}" multiple times'
                )
            ctx.auto.add(self._arg)
            ctx.bang_lines["auto"][self._arg] = self
        elif self._type == BangLine.TYPE_BOLT:
            if ctx.bolt_version is not None:
                raise LineError(self, "repeated definition of bolt version")
            ctx.bolt_version = self._arg
            ctx.bang_lines["bolt_version"] = self
        elif self._type == BangLine.TYPE_RESTART:
            if ctx.restarting:
                warnings.warn(  # noqa: B028
                    'Specified "!: ALLOW RESTART" multiple times'
                )
            ctx.restarting = True
            ctx.bang_lines["restarting"] = self
        elif self._type == BangLine.TYPE_CONCURRENT:
            if ctx.concurrent:
                warnings.warn(  # noqa: B028
                    'Specified "!: ALLOW CONCURRENT" multiple times'
                )
            ctx.concurrent = True
            ctx.bang_lines["concurrent"] = self
        elif self._type == BangLine.TYPE_HANDSHAKE:
            if ctx.handshake:
                warnings.warn(  # noqa: B028
                    'Specified "!: HANDSHAKE" multiple times'
                )
            ctx.handshake = self._arg
            ctx.bang_lines["handshake"] = self
        elif self._type == BangLine.TYPE_HANDSHAKE_DELAY:
            if ctx.handshake_delay is not None:
                warnings.warn(  # noqa: B028
                    'Specified "!: HANDSHAKE_DELAY" multiple times'
                )
            ctx.handshake_delay = self._arg
            ctx.bang_lines["handshake_delay"] = self
        elif self._type == BangLine.TYPE_PYTHON:
            ctx.bang_lines["python"].append(self)
            ctx.python.append(self._arg)
        if ctx.restarting and ctx.concurrent:
            warnings.warn(  # noqa: B028
                'Specified "!: ALLOW RESTART" and "!: ALLOW CONCURRENT" '
                "(concurrent scripts are implicitly restarting)"
            )


class MessageLine(Line):
    allow_jolt_wildcard = False
    always_parse = True

    def __new__(cls, line_number: int, raw_line, content: str):
        obj = super().__new__(cls, line_number, raw_line, content)
        if cls.always_parse:
            obj.parsed = cls._parse_line(obj)
        else:
            obj.parsed = None, []
        obj.jolt_parsed = None
        return obj

    @staticmethod
    def _parse_line(line):
        def splart(s):
            parts = s.split(maxsplit=1)
            while len(parts) < 2:
                parts.append("")
            return parts

        content = line.content
        name, data = splart(content.strip())
        fields = []
        decoder = json.JSONDecoder()
        while data:
            data = data.lstrip()
            try:
                decoded, end = decoder.raw_decode(data)
            except json.JSONDecodeError as e:
                raise LineError(
                    line,
                    "message fields must be white space separated json"
                ) from e
            fields.append(decoded)
            data = data[end:]
        return name, fields

    def parse_jolt(self, jolt_package):
        jolt_fields = []
        for field in self.parsed[1]:
            try:
                decoded = jolt_package.codec.decode(field)
            except (ValueError, AssertionError) as e:
                raise LineError(
                    self,
                    "message fields failed JOLT parser"
                ) from e
            decoded = self._jolt_to_struct(decoded)
            jolt_fields.append(decoded)
        self.jolt_parsed = self.parsed[0], jolt_fields
        return self.jolt_parsed

    def _jolt_to_struct(self, decoded):
        if isinstance(decoded, JoltWildcard):
            if not self.allow_jolt_wildcard:
                raise LineError(
                    self, "{} does not allow for JOLT wildcard values"
                          .format(self.__class__.__name__)
                )
            else:
                return decoded
        if isinstance(decoded, JoltType):
            return Structure.from_jolt_type(decoded)
        if isinstance(decoded, (list, tuple)):
            return type(decoded)(self._jolt_to_struct(d) for d in decoded)
        if isinstance(decoded, dict):
            return {k: self._jolt_to_struct(v) for k, v in decoded.items()}
        return decoded


class ClientLine(MessageLine):
    allow_jolt_wildcard = True

    def canonical(self):
        return " ".join(("C:", self.parsed[0],
                         *map(json.dumps, self.parsed[1])))

    def match_message(self, name, fields):
        assert self.jolt_parsed
        line_name, line_fields = self.jolt_parsed
        if line_name != name:
            return False
        return self._field_match(line_fields, fields)

    @classmethod
    def _dict_match(cls, should, is_):
        accepted_keys = set()
        for should_key in should:
            should_key_unescaped = re.sub(r"\\([\[\]\\\{\}])", r"\1",
                                          should_key)
            should_value = should[should_key]
            optional = re.match(r"^\[.*\]$", should_key)
            if optional:
                should_key_unescaped = should_key_unescaped[1:-1]
            ordered = re.match(r"^(?:\[.*\{\}\]|.*\{\})$", should_key)
            if ordered:
                should_key_unescaped = should_key_unescaped[:-2]
                if isinstance(should_value, list):
                    should_value = sorted(should_value)
            accepted_keys.add(should_key_unescaped)
            if should_key_unescaped in is_:
                is_value = is_[should_key_unescaped]
                if ordered and isinstance(is_value, list):
                    is_value = sorted(is_value)
                if not cls._field_match(should_value, is_value):
                    return False
            elif not optional:
                return False
        return not set(is_.keys()).difference(accepted_keys)

    @classmethod
    def _field_match(cls, should, is_):
        if isinstance(should, str):
            if should == "*":
                return True
            should = re.sub(r"\\([\\*])", r"\1", should)
        if isinstance(should, JoltWildcard):
            if isinstance(is_, Structure):
                return is_.match_jolt_wildcard(should)
            return type(is_) in should.types
        if isinstance(is_, Structure):
            return is_ == should
        if type(should) is not type(is_):
            return False
        if isinstance(should, (list, tuple)):
            if len(should) != len(is_):
                return False
            return all(cls._field_match(a, b)
                       for a, b in zip(should, is_))
        if isinstance(should, dict):
            return cls._dict_match(should, is_)
        if isinstance(should, float) and math.isnan(should):
            return isinstance(is_, float) and math.isnan(is_)
        return is_ == should


class AutoLine(ClientLine):
    def canonical(self):
        return " ".join(("A:", self.parsed[0],
                         *map(json.dumps, self.parsed[1])))


class ServerLine(MessageLine):
    always_parse = False

    def __new__(cls, *args, **kwargs):
        obj = super(ServerLine, cls).__new__(cls, *args, **kwargs)
        obj.command_match = re.match(r"^<(.+?)>(.*)$", obj.content)
        obj.is_command = bool(obj.command_match)
        if not obj.is_command:
            obj.parsed = cls._parse_line(obj)
        else:
            cls._verify_command(obj)
        return obj

    def parse_jolt(self, simple_jolt):
        if not self.is_command:
            super().parse_jolt(simple_jolt)
        else:
            self._verify_command(self)
        return self

    @staticmethod
    def _verify_command(obj):
        if obj.command_match:
            tag, args = obj.command_match.groups()
            args = args.strip()
            if tag == "EXIT":
                if args:
                    raise LineError(obj, "EXIT takes no arguments")
            elif tag == "NOOP":
                if args:
                    raise LineError(obj, "NOOP takes no arguments")
            elif tag == "RAW":
                try:
                    bytearray(int(_, 16) for _ in wrap(args, 2))
                except ValueError as e:
                    raise LineError(obj, "Invalid raw data") from e
            elif tag == "SLEEP":
                try:
                    f = float(args)
                    if f < 0:
                        raise LineError(obj, "Duration must be non-negative")
                except ValueError as e:
                    raise LineError(obj, "Invalid duration") from e
            elif tag == "ASSERT ORDER":
                if not args.strip():
                    return
                try:
                    f = float(args)
                    if f < 0:
                        raise LineError(obj, "Duration must be non-negative")
                except ValueError as e:
                    raise LineError(obj, "Invalid duration") from e
            else:
                raise LineError(obj, "Unknown command %r" % (tag,))

    def canonical(self):
        if self.is_command:
            return "S: {}".format(self.content)
        else:
            return " ".join(("S:", self.parsed[0],
                             *map(json.dumps, self.parsed[1])))

    def try_run_command(self, channel):
        if self.command_match:
            tag, args = self.command_match.groups()
            args = args.strip()
            if tag == "EXIT":
                raise ServerExit(
                    "server exit as part of the script: {}".format(self)
                )
            elif tag == "NOOP":
                channel.send_raw(b"\x00\x00")
            elif tag == "RAW":
                channel.send_raw(bytearray(int(_, 16) for _ in wrap(args, 2)))
            elif tag == "SLEEP":
                sleep(float(args))
            elif tag == "ASSERT ORDER":
                sleep(float(args.strip() or 1))
                channel.assert_no_input()
            else:
                raise ValueError("Unknown command %r" % (tag,))
            return True
        return False


class PythonLine(Line):
    def canonical(self):
        return "PY: {}".format(self.content)

    def exec(self, eval_context: EvalContext):
        eval_context.exec(self.content.strip())


class Block(abc.ABC):
    def __init__(self, line_number: int):
        self.line_number = line_number

    @abc.abstractmethod
    def accepted_messages(self, channel) -> List[ClientLine]:
        pass

    @abc.abstractmethod
    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        pass

    @abc.abstractmethod
    def assert_no_init(self):
        """Raise error if `init()` would send messages."""

    @abc.abstractmethod
    def done(self, channel) -> bool:
        pass

    def can_be_skipped(self, channel):
        return False

    @abc.abstractmethod
    def can_consume(self, channel) -> bool:
        pass

    @abc.abstractmethod
    def can_consume_after_reset(self, channel) -> bool:
        pass

    def consume(self, channel):
        assert self.try_consume(channel)

    @abc.abstractmethod
    def has_deterministic_end(self):
        pass

    @abc.abstractmethod
    def init(self, channel):
        pass

    @abc.abstractmethod
    def reset(self):
        pass

    @abc.abstractmethod
    def try_consume(self, channel) -> bool:
        pass

    @property
    @abc.abstractmethod
    def all_lines(self):
        pass

    @property
    @abc.abstractmethod
    def client_lines(self):
        pass

    @property
    @abc.abstractmethod
    def server_lines(self):
        pass

    @abc.abstractmethod
    def parse_jolt(self, simple_jolt):
        pass


class ClientBlock(Block):
    def __init__(self, lines: List[ClientLine], line_number: int):
        super().__init__(line_number)
        self.lines = lines
        self.index = 0

    def accepted_messages(self, channel) -> List[ClientLine]:
        return self.lines[self.index:(self.index + 1)]

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        return self.lines[0:1]

    def assert_no_init(self):
        return

    def can_consume(self, channel) -> bool:
        if self.done(channel):
            return False
        return channel.match_client_line(self.lines[self.index],
                                         channel.peek())

    def can_consume_after_reset(self, channel) -> bool:
        return self.lines and channel.match_client_line(self.lines[0],
                                                        channel.peek())

    def _consume(self, channel):
        channel.consume(self.lines[self.index].line_number)
        self.index += 1

    def done(self, channel):
        return self.index >= len(self.lines)

    def has_deterministic_end(self) -> bool:
        return True

    def init(self, channel):
        return

    def reset(self):
        self.index = 0

    def try_consume(self, channel) -> bool:
        if self.can_consume(channel):
            self._consume(channel)
            return True
        return False

    @property
    def all_lines(self):
        yield from map(deepcopy, self.lines)

    @property
    def client_lines(self):
        yield from map(deepcopy, self.lines)

    @property
    def server_lines(self):
        yield from ()

    def parse_jolt(self, simple_jolt):
        for line in self.lines:
            line.parse_jolt(simple_jolt)


class AutoBlock(ClientBlock):
    def __init__(self, line: AutoLine, line_number: int):
        # AutoBlocks always have exactly one line. E.g., this syntax is invalid
        #   A: HELLO "*"
        #      RESET
        # Instead, it must be
        #   A: HELLO "*"
        #   A: RESET
        # This is to avoid ambiguity when it comes to `?:`, `*:`, and `+:`
        # macros.
        super(AutoBlock, self).__init__([line], line_number)

    def _consume(self, channel):
        msg = channel.consume(self.lines[self.index].line_number)
        channel.auto_respond(msg)
        self.index += 1


class ServerBlock(Block):
    def __init__(self, lines: List[ServerLine], line_number: int):
        super().__init__(line_number)
        self.lines = lines
        self.index = 0

    def accepted_messages(self, channel) -> List[ClientLine]:
        return []

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        return []

    def assert_no_init(self):
        if self.lines:
            raise LineError(
                self.lines[0],
                "ambiguity of script does not allow for server response here"
            )

    def can_consume(self, channel) -> bool:
        return False

    def can_consume_after_reset(self, channel) -> bool:
        return False

    def done(self, channel):
        return self.index >= len(self.lines)

    def has_deterministic_end(self) -> bool:
        return True

    def init(self, channel):
        self.respond(channel)

    def respond(self, channel):
        while not self.done(channel):
            line = self.lines[self.index]
            if not line.try_run_command(channel):
                channel.send_server_line(line)
            self.index += 1

    def reset(self):
        self.index = 0

    def try_consume(self, channel) -> bool:
        return False

    @property
    def all_lines(self):
        yield from map(deepcopy, self.lines)

    @property
    def client_lines(self):
        yield from ()

    @property
    def server_lines(self):
        yield from map(deepcopy, self.lines)

    def parse_jolt(self, simple_jolt):
        for line in self.lines:
            line.parse_jolt(simple_jolt)


class PythonBlock(ServerBlock):
    def __init__(self, lines: List[PythonLine], line_number: int):
        super().__init__(lines, line_number)
        self.lines = lines
        self.index = 0

    def respond(self, channel):
        while not self.done(channel):
            line = self.lines[self.index]
            line.exec(channel.eval_context)
            self.index += 1

    def parse_jolt(self, simple_jolt):
        pass

    def assert_no_init(self):
        if self.lines:
            raise LineError(
                self.lines[0],
                "ambiguity of script does not allow for python execution here"
            )

    @property
    def server_lines(self):
        yield from ()


class AlternativeBlock(Block):
    def __init__(self, block_lists: List["BlockList"], line_number: int):
        super().__init__(line_number)
        self.block_lists = block_lists
        self.selection = None
        self.assert_no_init()

    def accepted_messages(self, channel) -> List[ClientLine]:
        if self.selection is None:
            return sum((b.accepted_messages(channel)
                        for b in self.block_lists), [])
        return self.block_lists[self.selection].accepted_messages(channel)

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        return sum((b.accepted_messages_after_reset(channel)
                    for b in self.block_lists),
                   [])

    def assert_no_init(self):
        for block in self.block_lists:
            block.assert_no_init()

    def can_be_skipped(self, channel):
        if self.selection is None:
            return any(b.can_be_skipped(channel) for b in self.block_lists)
        return self.block_lists[self.selection].can_be_skipped(channel)

    def can_consume(self, channel) -> bool:
        if self.selection is None:
            return any(b.can_consume(channel) for b in self.block_lists)
        return self.block_lists[self.selection].can_consume(channel)

    def can_consume_after_reset(self, channel) -> bool:
        return any(b.can_consume_after_reset(channel)
                   for b in self.block_lists)

    def done(self, channel):
        return (self.selection is not None
                and self.block_lists[self.selection].done(channel))

    def has_deterministic_end(self) -> bool:
        return all(b.has_deterministic_end() for b in self.block_lists)

    def init(self, channel):
        # self.assert_no_init()
        pass

    def reset(self):
        self.selection = None
        for block in self.block_lists:
            block.reset()

    def try_consume(self, channel) -> bool:
        if self.selection is not None:
            return self.block_lists[self.selection].try_consume(channel)
        for i in range(len(self.block_lists)):
            if self.block_lists[i].try_consume(channel):
                self.selection = i
                return True
        return False

    @property
    def all_lines(self):
        for block_list in self.block_lists:
            yield from block_list.all_lines

    @property
    def client_lines(self):
        for block_list in self.block_lists:
            yield from block_list.client_lines

    @property
    def server_lines(self):
        for block_list in self.block_lists:
            yield from block_list.server_lines

    def parse_jolt(self, simple_jolt):
        for block_list in self.block_lists:
            block_list.parse_jolt(simple_jolt)


class ParallelBlock(Block):
    def __init__(self, block_lists: List["BlockList"], line_number: int):
        super().__init__(line_number)
        self.block_lists = block_lists
        self.assert_no_init()

    def accepted_messages(self, channel) -> List[ClientLine]:
        return sum((b.accepted_messages(channel)
                    for b in self.block_lists), [])

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        return sum((b.accepted_messages_after_reset(channel)
                    for b in self.block_lists), [])

    def assert_no_init(self):
        for b in self.block_lists:
            b.assert_no_init()

    def done(self, channel) -> bool:
        return all(b.done(channel) for b in self.block_lists)

    def can_be_skipped(self, channel):
        return all(b.can_be_skipped(channel) for b in self.block_lists)

    def can_consume(self, channel) -> bool:
        return any(b.can_consume(channel) for b in self.block_lists)

    def can_consume_after_reset(self, channel) -> bool:
        return any(b.can_consume_after_reset(channel)
                   for b in self.block_lists)

    def has_deterministic_end(self) -> bool:
        return all(b.has_deterministic_end() for b in self.block_lists)

    def init(self, channel):
        # self.assert_no_init()
        pass

    def reset(self):
        for block in self.block_lists:
            block.reset()

    def try_consume(self, channel) -> bool:
        for block in self.block_lists:
            if block.try_consume(channel):
                return True
        return False

    @property
    def all_lines(self):
        for block_list in self.block_lists:
            yield from block_list.all_lines

    @property
    def client_lines(self):
        for block_list in self.block_lists:
            yield from block_list.client_lines

    @property
    def server_lines(self):
        for block_list in self.block_lists:
            yield from block_list.server_lines

    def parse_jolt(self, simple_jolt):
        for block_list in self.block_lists:
            block_list.parse_jolt(simple_jolt)


class OptionalBlock(Block):
    def __init__(self, block_list: "BlockList", line_number: int):
        super().__init__(line_number)
        self.started = False
        self.block_list = block_list
        self.assert_no_init()

    def accepted_messages(self, channel) -> List[ClientLine]:
        return self.block_list.accepted_messages(channel)

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        return self.block_list.accepted_messages_after_reset(channel)

    def assert_no_init(self):
        self.block_list.assert_no_init()

    def can_be_skipped(self, channel):
        if self.started:
            if self.block_list.has_deterministic_end():
                return self.block_list.done(channel)
            return self.block_list.can_be_skipped(channel)
        return True

    def can_consume(self, channel) -> bool:
        return self.block_list.can_consume(channel)

    def can_consume_after_reset(self, channel) -> bool:
        return self.block_list.can_consume_after_reset(channel)

    def done(self, channel) -> bool:
        if self.started and self.block_list.has_deterministic_end():
            return self.block_list.done(channel)
        raise RuntimeError("it's nondeterministic!")

    def has_deterministic_end(self) -> bool:
        if not self.started:
            return False
        return self.block_list.has_deterministic_end()

    def init(self, channel):
        # self.assert_no_init()
        pass

    def reset(self):
        self.started = False
        self.block_list.reset()

    def try_consume(self, channel) -> bool:
        if self.block_list.try_consume(channel):
            self.started = True
            return True
        return False

    @property
    def all_lines(self):
        return self.block_list.all_lines

    @property
    def client_lines(self):
        return self.block_list.client_lines

    @property
    def server_lines(self):
        return self.block_list.server_lines

    def parse_jolt(self, simple_jolt):
        self.block_list.parse_jolt(simple_jolt)


class _RepeatBlock(Block, abc.ABC):
    def __init__(self, block_list, line_number: int):
        super().__init__(line_number)
        self.in_block = False
        self.iteration_count = 0
        self.block_list = block_list
        self.assert_no_init()

    def accepted_messages(self, channel) -> List[ClientLine]:
        res = OrderedDict((m, True)
                          for m in self.block_list.accepted_messages(channel))
        if ((self.has_deterministic_end() and self.done(channel))
                or self.block_list.can_be_skipped(channel)):
            res.update(
                (m, True)
                for m in self.block_list.accepted_messages_after_reset(channel)
            )
        return list(res.keys())

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        return self.block_list.accepted_messages_after_reset(channel)

    def assert_no_init(self):
        self.block_list.assert_no_init()

    @abc.abstractmethod
    def can_be_skipped(self, channel):
        pass

    def _can_consume_deterministic(self, channel):
        if self.block_list.can_consume(channel):
            return True
        if self.block_list.done(channel):
            return self.block_list.can_consume_after_reset(channel)
        return False

    def _can_consume_nondeterministic(self, channel):
        if self.block_list.can_consume(channel):
            return True
        if self.block_list.can_be_skipped(channel):
            return self.block_list.can_consume_after_reset(channel)
        return False

    def can_consume(self, channel) -> bool:
        if self.block_list.has_deterministic_end():
            return self._can_consume_deterministic(channel)
        return self._can_consume_nondeterministic(channel)

    def can_consume_after_reset(self, channel) -> bool:
        return self.block_list.can_consume_after_reset(channel)

    def done(self, channel) -> bool:
        raise RuntimeError("it's nondeterministic!")

    def has_deterministic_end(self) -> bool:
        return False

    def init(self, channel):
        # self.assert_no_init()
        pass

    def reset(self):
        self.in_block = False
        self.iteration_count = 0
        self.block_list.reset()

    def _consume_after_jump_to_top(self, channel):
        self.block_list.reset()
        self.iteration_count += 1
        return self.try_consume(channel)

    def _try_consume_deterministic(self, channel):
        if self.block_list.done(channel):
            return self._consume_after_jump_to_top(channel)
        if self.block_list.try_consume(channel):
            self.in_block = not self.block_list.done(channel)
            return True
        return False

    def _try_consume_nondeterministic(self, channel):
        if self.block_list.try_consume(channel):
            self.in_block = not self.block_list.can_be_skipped(channel)
            return True
        elif (self.block_list.can_be_skipped(channel)
                and self.block_list.can_consume_after_reset(channel)):
            assert self._consume_after_jump_to_top(channel)
            return True
        return False

    def try_consume(self, channel) -> bool:
        if self.block_list.has_deterministic_end():
            return self._try_consume_deterministic(channel)
        return self._try_consume_nondeterministic(channel)

    @property
    def all_lines(self):
        return self.block_list.all_lines

    @property
    def client_lines(self):
        return self.block_list.client_lines

    @property
    def server_lines(self):
        return self.block_list.server_lines

    def parse_jolt(self, simple_jolt):
        self.block_list.parse_jolt(simple_jolt)


class Repeat0Block(_RepeatBlock):
    def can_be_skipped(self, channel):
        return not self.in_block


class Repeat1Block(_RepeatBlock):
    def can_be_skipped(self, channel):
        if self.in_block:
            return False
        return (
            self.block_list.can_be_skipped(channel)
            or self.iteration_count >= 1
        )


class ConditionalBlock(Block):
    def __init__(self, conditions: List[str], blocks: List[Block],
                 line_number: int):
        super().__init__(line_number)
        self.conditions = conditions
        self.blocks = blocks
        self.selection = None

    def _get_selected_block(
        self, channel, selection, probing
    ) -> Optional[int]:
        if selection is not None:
            return selection
        for i, condition in enumerate(self.conditions):
            if channel.eval_context.eval(condition, probing=probing):
                return i
        if len(self.blocks) > len(self.conditions):
            return len(self.conditions)
        return None

    def _probe_selection(self, channel, selection) -> Optional[Block]:
        selection = self._get_selected_block(channel, selection, probing=True)
        if selection is None:
            return None
        return self.blocks[selection]

    def _get_selection(self, channel, selection) -> Optional[Block]:
        selection = self._get_selected_block(channel, selection, probing=False)
        self.selection = selection
        if selection is None:
            return None
        return self.blocks[selection]

    def accepted_messages(self, channel) -> List[ClientLine]:
        block = self._probe_selection(channel, self.selection)
        if not block:
            return []
        return block.accepted_messages(channel)

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        block = self._probe_selection(channel, None)
        if not block:
            return []
        return block.accepted_messages_after_reset(channel)

    def assert_no_init(self):
        return

    def done(self, channel) -> bool:
        block = self._probe_selection(channel, self.selection)
        if not block:
            return True
        return block.done(channel)

    def can_be_skipped(self, channel):
        block = self._probe_selection(channel, self.selection)
        if not block:
            return True
        return block.can_be_skipped(channel)

    def can_consume(self, channel) -> bool:
        block = self._probe_selection(channel, self.selection)
        if not block:
            return False
        return block.can_consume(channel)

    def can_consume_after_reset(self, channel) -> bool:
        block = self._probe_selection(channel, None)
        if not block:
            return False
        return block.can_consume_after_reset(channel)
        pass

    def has_deterministic_end(self):
        return all(b.has_deterministic_end() for b in self.blocks)

    def init(self, channel):
        block = self._get_selection(channel, self.selection)
        if not block:
            return
        block.init(channel)

    def reset(self):
        self.selection = None
        for block in self.blocks:
            block.reset()

    def try_consume(self, channel) -> bool:
        block = self._get_selection(channel, self.selection)
        if not block:
            return False
        return block.try_consume(channel)

    @property
    def all_lines(self):
        for block in self.blocks:
            yield from block.all_lines

    @property
    def client_lines(self):
        for block in self.blocks:
            yield from block.client_lines

    @property
    def server_lines(self):
        for block in self.blocks:
            yield from block.server_lines

    def parse_jolt(self, simple_jolt):
        for block in self.blocks:
            block.parse_jolt(simple_jolt)


class BlockList(Block):
    def __init__(self, blocks: List[Block], line_number: int):
        super().__init__(line_number)
        self.blocks = blocks
        self.index = 0
        for prev_block, next_block in zip(self.blocks, self.blocks[1:]):
            if not prev_block.has_deterministic_end():
                next_block.assert_no_init()

    def accepted_messages(self, channel) -> List[ClientLine]:
        res = []
        if self.index >= len(self.blocks):
            return res

        for i in range(self.index, len(self.blocks)):
            res += self.blocks[i].accepted_messages(channel)
            if not self.blocks[i].can_be_skipped(channel):
                break
        return res

    def accepted_messages_after_reset(self, channel) -> List[ClientLine]:
        res = []
        if not self.blocks:
            return res

        for i in range(len(self.blocks)):
            res += self.blocks[i].accepted_messages_after_reset(channel)
            if not self.blocks[i].can_be_skipped(channel):
                break
        return res

    def assert_no_init(self):
        self.blocks[0].assert_no_init()

    def can_be_skipped(self, channel):
        return all(b.can_be_skipped(channel)
                   for b in self.blocks[self.index:len(self.blocks)])

    def can_consume(self, channel) -> bool:
        for i in range(self.index, len(self.blocks)):
            if self.blocks[i].can_consume(channel):
                return True
            if not self.blocks[i].can_be_skipped(channel):
                break
        return False

    def can_consume_after_reset(self, channel) -> bool:
        for i in range(len(self.blocks)):
            if self.blocks[i].can_consume_after_reset(channel):
                return True
            if not self.blocks[i].can_be_skipped(channel):
                break
        return False

    def done(self, channel) -> bool:
        if not self.has_deterministic_end():
            raise RuntimeError("it's nondeterministic!")
        return self.index >= len(self.blocks)

    def has_deterministic_end(self) -> bool:
        return not self.blocks or self.blocks[-1].has_deterministic_end()

    def init(self, channel):
        while self.index < len(self.blocks):
            block = self.blocks[self.index]
            block.init(channel)
            if not block.has_deterministic_end() or not block.done(channel):
                break
            self.index += 1

    def reset(self):
        for block in self.blocks:
            block.reset()
        self.index = 0

    def try_consume(self, channel) -> bool:
        for i in range(self.index, len(self.blocks)):
            block = self.blocks[i]
            if block.try_consume(channel):
                self.index = i
                while block.has_deterministic_end() and block.done(channel):
                    self.index += 1
                    if self.index < len(self.blocks):
                        block = self.blocks[self.index]
                        block.init(channel)
                    else:
                        break
                return True
            if not block.can_be_skipped(channel):
                break
        return False

    @property
    def all_lines(self):
        for block in self.blocks:
            yield from block.all_lines

    @property
    def client_lines(self):
        for block in self.blocks:
            yield from block.client_lines

    @property
    def server_lines(self):
        for block in self.blocks:
            yield from block.server_lines

    def parse_jolt(self, simple_jolt):
        for block in self.blocks:
            block.parse_jolt(simple_jolt)


class ScriptFailure(RuntimeError):
    pass


class ScriptDeviation(ScriptFailure):
    def __init__(self, expected_lines: List[Line], received: Line):
        assert expected_lines
        self.expected_lines = expected_lines
        self.received = received

    def __str__(self):
        res = "Expected"
        if len(self.expected_lines) > 1:
            res += " one of"
        res += ":\n"
        res += "\n".join(map(str, self.expected_lines))
        res += "\n\nReceived:\n" + str(self.received)
        res += "\n => " + repr(self.received)
        return res


class ScriptContext:
    def __init__(self):
        self.bolt_version = None
        self.auto = set()
        self.restarting = False
        self.concurrent = False
        self.handshake = None
        self.handshake_delay = None
        self.python = []
        self.bang_lines = {
            "bolt_version": None,
            "auto": {},
            "restarting": None,
            "concurrent": None,
            "handshake": None,
            "handshake_delay": None,
            "python": [],
        }

    def create_eval_context(self) -> EvalContext:
        context = EvalContext()
        for cmd in self.python:
            context.exec(cmd)
        return context


class Script:
    def __init__(self, bang_lines: List[BangLine], block_list: BlockList,
                 filename=None):
        self.context = ScriptContext()
        self._consume_bang_lines(bang_lines)
        self.block_list = block_list
        self.filename = filename or ""
        self._skipped = False
        self._set_bolt_protocol()
        self._post_process()
        self._verify_script()
        self._lock = CopyableRLock()

    def _set_bolt_protocol(self):
        try:
            self._bolt_protocol = get_bolt_protocol(self.context.bolt_version)
        except BoltMissingVersionError as e:
            raise lark.GrammarError(
                'Missing bolt version bang line (e.g. "!: BOLT 4.3")'
            ) from e
        except BoltUnknownVersionError as e:
            raise LineError(
                self.context.bang_lines["bolt_version"], *e.args[:1]
            ) from e

    def _post_process(self):
        self.block_list.parse_jolt(self._bolt_protocol.get_jolt_package())

    def _verify_script(self):
        try:
            verify_script_messages(self)
        except BoltUnknownMessageError as e:
            raise LineError(e.line, e.msg) from e

    def _consume_bang_lines(self, bang_lines):
        for bl in bang_lines:
            bl.update_context(self.context)

    def init(self, channel):
        with self._lock:
            self.block_list.init(channel)

    def consume(self, channel):
        with self._lock:
            if not self.block_list.try_consume(channel):
                if not channel.try_auto_consume(self.context.auto):
                    raise ScriptDeviation(
                        self.block_list.accepted_messages(channel),
                        channel.peek()
                    )

    def done(self, channel):
        with self._lock:
            if self._skipped:
                return True
            if self.block_list.has_deterministic_end():
                return self.block_list.done(channel)
            return False

    def try_skip_to_end(self, channel):
        with self._lock:
            if self.block_list.can_be_skipped(channel):
                self._skipped = True

    @property
    def all_lines(self):
        return self.block_list.all_lines

    @property
    def client_lines(self):
        return self.block_list.client_lines

    @property
    def server_lines(self):
        return self.block_list.server_lines


class ScriptTransformer(lark.Transformer):
    @lark.v_args(meta=True)
    def bang_line(self, meta, children):
        return BangLine(meta.line, "".join(children),
                        children[-1].strip())

    @lark.v_args(meta=True)
    def client_line(self, meta, children):
        return ClientLine(meta.line, "".join(children),
                          children[-1].strip())

    @lark.v_args(meta=True)
    def auto_line(self, meta, children):
        return AutoLine(meta.line, "".join(children),
                        children[-1].strip())

    @lark.v_args(meta=True)
    def server_line(self, meta, children):
        return ServerLine(meta.line, "".join(children),
                          children[-1].strip())

    @lark.v_args(meta=True)
    def python_line(self, meta, children):
        return PythonLine(meta.line, "".join(children),
                          children[-1].strip())

    def start(self, children):
        bang_lines = []
        block_list = None
        for child in children:
            if isinstance(child, BangLine):
                bang_lines.append(child)
            elif isinstance(child, BlockList):
                block_list = child
                break
        return Script(bang_lines, block_list)

    @lark.v_args(meta=True)
    def block_list(self, meta, children):
        blocks = []
        for child in children:
            if isinstance(child, lark.Token):
                continue
            if (blocks
                    and ((child.__class__ == ClientBlock
                          and blocks[-1].__class__ == ClientBlock)
                         or (child.__class__ == ServerBlock
                             and blocks[-1].__class__ == ServerBlock))):
                blocks[-1].lines.extend(child.lines)
            else:
                blocks.append(child)

        return BlockList(blocks, meta.line)

    @lark.v_args(meta=True)
    def client_block(self, meta, children):
        return ClientBlock(
            [
                child for child in children
                if child.__class__ == ClientLine
            ],
            meta.line
        )

    @lark.v_args(meta=True)
    def auto_block(self, meta, children):
        assert len(children) == 1
        assert children[0].__class__ == AutoLine
        return AutoBlock(children[0], meta.line)

    def _wrapped_auto_block(self, wrapper, meta, children):
        assert len(children) == 1
        assert children[0].__class__ == AutoLine
        return wrapper(
            BlockList(
                [AutoBlock(children[0], meta.line)],
                meta.line
            ),
            meta.line
        )

    @lark.v_args(meta=True)
    def auto_optional_block(self, meta, children):
        return self._wrapped_auto_block(OptionalBlock, meta, children)

    @lark.v_args(meta=True)
    def auto_loop0_block(self, meta, children):
        return self._wrapped_auto_block(Repeat0Block, meta, children)

    @lark.v_args(meta=True)
    def auto_loop1_block(self, meta, children):
        return self._wrapped_auto_block(Repeat1Block, meta, children)

    @lark.v_args(meta=True)
    def server_block(self, meta, children):
        return ServerBlock(
            [
                child for child in children
                if child.__class__ == ServerLine
            ],
            meta.line
        )

    @lark.v_args(meta=True)
    def python_block(self, meta, children):
        return PythonBlock(
            [
                child for child in children
                if child.__class__ == PythonLine
            ],
            meta.line
        )

    @lark.v_args()
    def simple_block(self, children):
        children = [c for c in children if not isinstance(c, lark.Token)]
        assert len(children) == 1
        assert isinstance(children[0], BlockList)
        return children[0]

    @lark.v_args(meta=True)
    def alternative_block(self, meta, children):
        return AlternativeBlock(
            [c for c in children if not isinstance(c, lark.Token)],
            meta.line
        )

    @lark.v_args(meta=True)
    def optional_block(self, meta, children):
        return OptionalBlock(children[2], meta.line)

    @lark.v_args(meta=True)
    def parallel_block(self, meta, children):
        return ParallelBlock(
            [c for c in children if not isinstance(c, lark.Token)],
            meta.line
        )

    @lark.v_args(meta=True)
    def repeat_0_block(self, meta, children):
        return Repeat0Block(children[2], meta.line)

    @lark.v_args(meta=True)
    def repeat_1_block(self, meta, children):
        return Repeat1Block(children[2], meta.line)

    @lark.v_args(meta=True)
    def conditional_block(self, meta, children):
        conditions = [children[0].children[-1].strip()]
        blocks = [children[2]]
        for child in children[4:]:
            if isinstance(child, lark.Tree) and child.data == "elif_line":
                conditions.append(child.children[-1].strip())
            elif isinstance(child, Block):
                blocks.append(child)

        return ConditionalBlock(conditions, blocks, meta.line)


def parse(script: str, substitutions: Optional[dict] = None) -> Script:
    if substitutions:
        for match, replacement in substitutions.items():
            script = script.replace(match, replacement)
    return ScriptTransformer().transform(parser.parse(script))


def parse_file(filename):
    with open(filename, encoding="utf-8") as fd:
        try:
            script = parse(fd.read())
        except Exception:
            print("Error while parsing %s" % filename, file=sys.stderr)
            raise
    script.filename = filename
    return script
