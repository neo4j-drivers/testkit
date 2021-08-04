import abc
from collections import OrderedDict
from copy import deepcopy
import json
import math
from os import path
import re
from textwrap import wrap
import threading
from time import sleep
from typing import (
    List,
    Optional
)
import warnings

import lark

from .bolt_protocol import verify_script_messages
from .errors import (
    BoltMissingVersion,
    BoltUnknownMessage,
    BoltUnknownVersion,
    ServerExit,
)
from .packstream import Structure
from .simple_jolt.transformers import decode as jolt_decode
from .simple_jolt import types as jolt_types


def load_parser():
    with open(path.join(path.dirname(__file__), "grammar.lark"), "r") as fd:
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
    allow_jolt_wildcard = False

    def __new__(cls, line_number: int, raw_line, content: str):
        obj = super(Line, cls).__new__(cls, raw_line)
        obj.line_number = line_number
        obj.content = content
        return obj

    def __str__(self):
        return "({:3}) {}".format(self.line_number, super(Line, self).__str__())

    def __repr__(self):
        return "<{}>{}".format(self.__class__.__name__, self.__str__())

    def __getnewargs__(self):
        return self.line_number, super(Line, self).__str__(), self.content

    @abc.abstractmethod
    def canonical(self):
        pass

    @classmethod
    def parse_line(cls, line):
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
            try:
                decoded = jolt_decode(decoded)
            except (ValueError, AssertionError) as e:
                raise LineError(
                    line,
                    "message fields failed JOLT parser"
                ) from e
            decoded = cls._jolt_to_struct(line, decoded)
            fields.append(decoded)
            data = data[end:]
        return name, fields

    @classmethod
    def _jolt_to_struct(cls, line, decoded):
        if isinstance(decoded, jolt_types.JoltWildcard):
            if not cls.allow_jolt_wildcard:
                raise LineError(
                    line, "{} does not allow for JOLT wildcard values"
                          .format(cls.__name__)
                )
            else:
                return decoded
        if isinstance(decoded, jolt_types.JoltType):
            return Structure.from_jolt_type(decoded)
        if isinstance(decoded, (list, tuple)):
            return type(decoded)(cls._jolt_to_struct(line, d)
                                 for d in decoded)
        if isinstance(decoded, dict):
            return {k: cls._jolt_to_struct(line, v)
                    for k, v in decoded.items()}
        return decoded


class BangLine(Line):
    TYPE_AUTO = "auto"
    TYPE_BOLT = "bolt"
    TYPE_RESTART = "restart"
    TYPE_CONCURRENT = "concurrent"
    TYPE_HANDSHAKE = "handshake"

    def __new__(cls, *args, **kwargs):
        obj = super(BangLine, cls).__new__(cls, *args, **kwargs)
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
                    "'HANDSHAKE 00 FF 02 04 F0'")
            obj._arg = bytearray(int(b, 16) for b in wrap(arg, 2))
        else:
            raise LineError(obj, 'unsupported Bang line: "{}"'.format(obj))
        return obj

    def canonical(self):
        return "!: {}".format(self.content)

    def update_context(self, ctx: "ScriptContext"):
        if self._type == BangLine.TYPE_AUTO:
            if self._arg in ctx.auto:
                warnings.warn(
                    'Specified AUTO for "{}" multiple times'.format(self._arg)
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
                warnings.warn('Specified "!: ALLOW RESTART" multiple times')
            ctx.restarting = True
            ctx.bang_lines["restarting"] = self
        elif self._type == BangLine.TYPE_CONCURRENT:
            if ctx.concurrent:
                warnings.warn('Specified "!: ALLOW CONCURRENT" multiple times')
            ctx.concurrent = True
            ctx.bang_lines["concurrent"] = self
        elif self._type == BangLine.TYPE_HANDSHAKE:
            if ctx.handshake:
                warnings.warn('Specified "!: HANDSHAKE" multiple times')
            ctx.handshake = self._arg
            ctx.bang_lines["handshake"] = self
        if ctx.restarting and ctx.concurrent:
            warnings.warn(
                'Specified "!: ALLOW RESTART" and "!: ALLOW CONCURRENT" '
                '(concurrent scripts are implicitly restarting)'
            )


class ClientLine(Line):
    allow_jolt_wildcard = True

    def __new__(cls, *args, **kwargs):
        obj = super(ClientLine, cls).__new__(cls, *args, **kwargs)
        obj.parsed = cls.parse_line(obj)
        return obj

    def canonical(self):
        return " ".join(("C:", self.parsed[0],
                         *map(json.dumps, self.parsed[1])))

    def match(self, msg):
        tag, fields = self.parsed
        if tag != msg.name:
            return False
        return ClientLine.field_match(fields, msg.fields)

    @staticmethod
    def _dict_match(should, is_):
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
                if not ClientLine.field_match(should_value, is_value):
                    return False
            elif not optional:
                return False
        return not set(is_.keys()).difference(accepted_keys)

    @staticmethod
    def field_match(should, is_):
        if isinstance(should, str):
            if should == "*":
                return True
            should = re.sub(r"\\([\\*])", r"\1", should)
        if isinstance(should, jolt_types.JoltWildcard):
            if isinstance(is_, Structure):
                return is_.match_jolt_wildcard(should)
            return type(is_) in should.types
        if isinstance(is_, Structure):
            return is_ == should
        if type(should) != type(is_):
            return False
        if isinstance(should, (list, tuple)):
            if len(should) != len(is_):
                return False
            return all(ClientLine.field_match(a, b)
                       for a, b in zip(should, is_))
        if isinstance(should, dict):
            return ClientLine._dict_match(should, is_)
        if isinstance(should, float) and math.isnan(should):
            return isinstance(is_, float) and math.isnan(is_)
        return is_ == should


class AutoLine(ClientLine):
    allow_jolt_wildcard = False

    def __new__(cls, *args, **kwargs):
        obj = super(AutoLine, cls).__new__(cls, *args, **kwargs)
        obj.parsed = cls.parse_line(obj)
        if obj.parsed[1]:
            raise LineError(obj, "Auto-Line does not fields.")
        return obj

    def canonical(self):
        return " ".join(("A:", self.parsed[0]))

    def match(self, msg):
        tag, fields = self.parsed
        return tag == msg.name


class ServerLine(Line):
    def __new__(cls, *args, **kwargs):
        obj = super(ServerLine, cls).__new__(cls, *args, **kwargs)
        obj.command_match = re.match(r"^<(\S+)>(.*)$", obj.content)
        if not obj.command_match:
            obj.parsed = cls.parse_line(obj)
        else:
            if not cls._transform_auto(obj):
                obj.parsed = None, []
                cls._verify_command(obj)
        return obj

    @staticmethod
    def _transform_auto(obj):
        if not obj.command_match:
            return False
        tag, args = obj.command_match.groups()
        if tag != "AUTO":
            return False
        pass

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
            else:
                raise LineError(obj, "Unknown command %r" % (tag,))

    def canonical(self):
        if self.parsed is None:
            return "S: {}".format(self.content)
        else:
            return " ".join(("S:", self.parsed[0],
                             *map(json.dumps, self.parsed[1])))

    def try_run_command(self, channel):
        if self.command_match:
            tag, args = self.command_match.groups()
            args = args.strip()
            if tag == "EXIT":
                raise ServerExit("server exit as part of the script: {}".format(
                    self
                ))
            elif tag == "NOOP":
                channel.send_raw(b"\x00\x00")
            elif tag == "RAW":
                channel.send_raw(bytearray(int(_, 16) for _ in wrap(args, 2)))
            elif tag == "SLEEP":
                sleep(float(args))
            else:
                raise ValueError("Unknown command %r" % (tag,))
            return True
        return False


class Block(abc.ABC):
    def __init__(self, line_number: int):
        self.line_number = line_number

    @abc.abstractmethod
    def accepted_messages(self) -> List[ClientLine]:
        pass

    @abc.abstractmethod
    def accepted_messages_after_reset(self) -> List[ClientLine]:
        pass

    @abc.abstractmethod
    def assert_no_init(self):
        """ raise error if init would send messages """

    @abc.abstractmethod
    def done(self) -> bool:
        pass

    def can_be_skipped(self):
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


class ClientBlock(Block):
    def __init__(self, lines: List[ClientLine], line_number: int):
        super().__init__(line_number)
        self.lines = lines
        self.index = 0

    def accepted_messages(self) -> List[ClientLine]:
        return self.lines[self.index:(self.index + 1)]

    def accepted_messages_after_reset(self) -> List[ClientLine]:
        return self.lines[0:1]

    def assert_no_init(self):
        return

    def can_consume(self, channel) -> bool:
        if self.done():
            return False
        return self.lines[self.index].match(channel.peek())

    def can_consume_after_reset(self, channel) -> bool:
        return self.lines and self.lines[0].match(channel.peek())

    def _consume(self, channel):
        channel.consume(self.lines[self.index].line_number)
        self.index += 1

    def done(self):
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


class AutoBlock(ClientBlock):
    def _consume(self, channel):
        msg = channel.consume(self.lines[self.index].line_number)
        channel.auto_respond(msg)
        self.index += 1


class ServerBlock(Block):
    def __init__(self, lines: List[ServerLine], line_number: int):
        super().__init__(line_number)
        self.lines = lines
        self.index = 0

    def accepted_messages(self) -> List[ClientLine]:
        return []

    def accepted_messages_after_reset(self) -> List[ClientLine]:
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

    def done(self):
        return self.index >= len(self.lines)

    def has_deterministic_end(self) -> bool:
        return True

    def init(self, channel):
        self.respond(channel)

    def respond(self, channel):
        while not self.done():
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


class AlternativeBlock(Block):
    def __init__(self, block_lists: List["BlockList"], line_number: int):
        super().__init__(line_number)
        self.block_lists = block_lists
        self.selection = None
        self.assert_no_init()

    def accepted_messages(self) -> List[ClientLine]:
        if self.selection is None:
            return sum((b.accepted_messages() for b in self.block_lists), [])
        return self.block_lists[self.selection].accepted_messages()

    def accepted_messages_after_reset(self) -> List[ClientLine]:
        return sum((b.accepted_messages_after_reset()
                    for b in self.block_lists),
                   [])

    def assert_no_init(self):
        for block in self.block_lists:
            block.assert_no_init()

    def can_be_skipped(self):
        if self.selection is None:
            return any(b.can_be_skipped() for b in self.block_lists)
        return self.block_lists[self.selection].can_be_skipped()

    def can_consume(self, channel) -> bool:
        if self.selection is None:
            return any(b.can_consume(channel) for b in self.block_lists)
        return self.block_lists[self.selection].can_consume(channel)

    def can_consume_after_reset(self, channel) -> bool:
        return any(b.can_consume_after_reset(channel) for b in self.block_lists)

    def done(self):
        return (self.selection is not None
                and self.block_lists[self.selection].done())

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


class ParallelBlock(Block):
    def __init__(self, block_lists: List["BlockList"], line_number: int):
        super().__init__(line_number)
        self.block_lists = block_lists
        self.assert_no_init()

    def accepted_messages(self) -> List[ClientLine]:
        return sum((b.accepted_messages() for b in self.block_lists), [])

    def accepted_messages_after_reset(self) -> List[ClientLine]:
        return sum((b.accepted_messages_after_reset()
                    for b in self.block_lists),
                   [])

    def assert_no_init(self):
        for b in self.block_lists:
            b.assert_no_init()

    def done(self) -> bool:
        return all(b.done() for b in self.block_lists)

    def can_be_skipped(self):
        return all(b.can_be_skipped() for b in self.block_lists)

    def can_consume(self, channel) -> bool:
        return any(b.can_consume(channel) for b in self.block_lists)

    def can_consume_after_reset(self, channel) -> bool:
        return any(b.can_consume_after_reset(channel) for b in self.block_lists)

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


class OptionalBlock(Block):
    def __init__(self, block_list: "BlockList", line_number: int):
        super().__init__(line_number)
        self.started = False
        self.block_list = block_list
        self.assert_no_init()

    def accepted_messages(self) -> List[ClientLine]:
        return self.block_list.accepted_messages()

    def accepted_messages_after_reset(self) -> List[ClientLine]:
        return self.block_list.accepted_messages_after_reset()

    def assert_no_init(self):
        self.block_list.assert_no_init()

    def can_be_skipped(self):
        if self.started:
            if self.block_list.has_deterministic_end():
                return self.block_list.done()
            return self.block_list.can_be_skipped()
        return True

    def can_consume(self, channel) -> bool:
        return self.block_list.can_consume(channel)

    def can_consume_after_reset(self, channel) -> bool:
        return self.block_list.can_consume_after_reset(channel)

    def done(self) -> bool:
        if self.started and self.block_list.has_deterministic_end():
            return self.block_list.done()
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


class _RepeatBlock(Block, abc.ABC):
    def __init__(self, block_list, line_number: int):
        super().__init__(line_number)
        self.in_block = False
        self.iteration_count = 0
        self.block_list = block_list
        self.assert_no_init()

    def accepted_messages(self) -> List[ClientLine]:
        res = OrderedDict((m, True)
                          for m in self.block_list.accepted_messages())
        if ((self.has_deterministic_end() and self.done())
                or self.block_list.can_be_skipped()):
            res.update((m, True)
                       for m in self.block_list.accepted_messages_after_reset())
        return list(res.keys())

    def accepted_messages_after_reset(self) -> List[ClientLine]:
        return self.block_list.accepted_messages_after_resets()

    def assert_no_init(self):
        self.block_list.assert_no_init()

    @abc.abstractmethod
    def can_be_skipped(self):
        pass

    def can_consume(self, channel) -> bool:
        return self.block_list.can_consume(channel)

    def can_consume_after_reset(self, channel) -> bool:
        return self.block_list.can_consume_after_reset(channel)

    def done(self) -> bool:
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
        if self.block_list.done():
            return self._consume_after_jump_to_top(channel)
        if self.block_list.try_consume(channel):
            self.in_block = not self.block_list.done()
            return True
        return False

    def _try_consume_nondeterministic(self, channel):
        if self.block_list.try_consume(channel):
            self.in_block = not self.block_list.can_be_skipped()
            return True
        elif (self.block_list.can_be_skipped()
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


class Repeat0Block(_RepeatBlock):
    def can_be_skipped(self):
        return not self.in_block


class Repeat1Block(_RepeatBlock):
    def can_be_skipped(self):
        if self.in_block:
            return False
        return self.block_list.can_be_skipped() or self.iteration_count >= 1


class BlockList(Block):
    def __init__(self, blocks: List[Block], line_number: int):
        super().__init__(line_number)
        self.blocks = blocks
        self.index = 0
        for prev_block, next_block in zip(self.blocks, self.blocks[1:]):
            if not prev_block.has_deterministic_end():
                next_block.assert_no_init()

    def accepted_messages(self) -> List[ClientLine]:
        res = []
        if self.index >= len(self.blocks):
            return res

        for i in range(self.index, len(self.blocks)):
            res += self.blocks[i].accepted_messages()
            if not self.blocks[i].can_be_skipped():
                break
        return res

    def accepted_messages_after_reset(self) -> List[ClientLine]:
        res = []
        if not self.blocks:
            return res

        for i in range(len(self.blocks)):
            res += self.blocks[i].accepted_messages_after_reset()
            if not self.blocks[i].can_be_skipped():
                break
        return res

    def assert_no_init(self):
        self.blocks[0].assert_no_init()

    def can_be_skipped(self):
        return all(b.can_be_skipped()
                   for b in self.blocks[self.index:len(self.blocks)])

    def can_consume(self, channel) -> bool:
        for i in range(self.index, len(self.blocks)):
            if self.blocks[i].can_consume(channel):
                return True
            if not self.blocks[i].can_be_skipped():
                break
        return False

    def can_consume_after_reset(self, channel) -> bool:
        for i in range(len(self.blocks)):
            if self.blocks[i].can_consume_after_reset(channel):
                return True
            if not self.blocks[i].can_be_skipped():
                break
        return False

    def done(self) -> bool:
        if not self.has_deterministic_end():
            raise RuntimeError("it's nondeterministic!")
        return self.index >= len(self.blocks)

    def has_deterministic_end(self) -> bool:
        return self.blocks[-1].has_deterministic_end()

    def init(self, channel):
        self.blocks[0].init(channel)

    def reset(self):
        for block in self.blocks:
            block.reset()
        self.index = 0

    def try_consume(self, channel) -> bool:
        for i in range(self.index, len(self.blocks)):
            block = self.blocks[i]
            if block.try_consume(channel):
                self.index = i
                while block.has_deterministic_end() and block.done():
                    self.index += 1
                    if self.index < len(self.blocks):
                        block = self.blocks[self.index]
                        block.init(channel)
                    else:
                        break
                return True
            if not block.can_be_skipped():
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
        return res


class ScriptContext:
    def __init__(self):
        self.bolt_version = None
        self.auto = set()
        self.restarting = False
        self.concurrent = False
        self.handshake = None
        self.bang_lines = {
            "bolt_version": None,
            "auto": {},
            "restarting": None,
            "concurrent": None,
            "handshake": None,
        }


class Script:
    def __init__(self, bang_lines: List[BangLine], block_list: BlockList,
                 filename=None):
        self.context = ScriptContext()
        self._consume_bang_lines(bang_lines)
        self.block_list = block_list
        self.filename = filename or ""
        self._skipped = False
        self._verify_script()
        self._lock = CopyableRLock()

    def _verify_script(self):
        try:
            verify_script_messages(self)
        except BoltMissingVersion as e:
            raise lark.GrammarError(
                'Missing bolt version bang line (e.g. "!: BOLT 4.3")'
            ) from e
        except BoltUnknownMessage as e:
            raise LineError(e.line, e.msg) from e
        except BoltUnknownVersion as e:
            raise LineError(
                self.context.bang_lines["bolt_version"], *e.args[:1]
            ) from e

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
                    raise ScriptDeviation(self.block_list.accepted_messages(),
                                          channel.peek())

    def done(self):
        with self._lock:
            if self._skipped:
                return True
            if self.block_list.has_deterministic_end():
                return self.block_list.done()
            return False

    def try_skip_to_end(self):
        with self._lock:
            if self.block_list.can_be_skipped():
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
    @lark.v_args(tree=True)
    def bang_line(self, tree):
        return BangLine(tree.line, "".join(tree.children),
                        tree.children[-1].strip())

    @lark.v_args(tree=True)
    def client_line(self, tree):
        return ClientLine(tree.line, "".join(tree.children),
                          tree.children[-1].strip())

    @lark.v_args(tree=True)
    def auto_line(self, tree):
        return AutoLine(tree.line, "".join(tree.children),
                        tree.children[-1].strip())

    @lark.v_args(tree=True)
    def server_line(self, tree):
        return ServerLine(tree.line, "".join(tree.children),
                          tree.children[-1].strip())

    @lark.v_args(tree=True)
    def start(self, tree):
        bang_lines = []
        block_list = None
        for child in tree.children:
            if isinstance(child, BangLine):
                bang_lines.append(child)
            elif isinstance(child, BlockList):
                block_list = child
                break
        return Script(bang_lines, block_list)

    @lark.v_args(tree=True)
    def block_list(self, tree):
        blocks = []
        for child in tree.children:
            if isinstance(child, lark.Token):
                continue
            if (blocks
                    and ((isinstance(child, ClientBlock)
                          and isinstance(blocks[-1], ClientBlock))
                         or (isinstance(child, ServerBlock)
                             and isinstance(blocks[-1], ServerBlock)))):
                blocks[-1].lines.extend(child.lines)
            else:
                blocks.append(child)

        return BlockList(blocks, tree.line)

    @lark.v_args(tree=True)
    def client_block(self, tree):
        return ClientBlock(
            [child for child in tree.children if isinstance(child, ClientLine)],
            tree.line
        )

    @lark.v_args(tree=True)
    def auto_block(self, tree):
        return AutoBlock(
            [child for child in tree.children if isinstance(child, AutoLine)],
            tree.line
        )

    def _wrapped_auto_block(self, wrapper, tree):
        return wrapper(
            BlockList(
                [
                    AutoBlock(
                        [child for child in tree.children if
                         isinstance(child, AutoLine)],
                        tree.line
                    )
                ],
                tree.line
            ),
            tree.line
        )

    @lark.v_args(tree=True)
    def auto_optional_block(self, tree):
        return self._wrapped_auto_block(OptionalBlock, tree)

    @lark.v_args(tree=True)
    def auto_loop0_block(self, tree):
        return self._wrapped_auto_block(Repeat0Block, tree)

    @lark.v_args(tree=True)
    def auto_loop1_block(self, tree):
        return self._wrapped_auto_block(Repeat1Block, tree)

    @lark.v_args(tree=True)
    def server_block(self, tree):
        return ServerBlock(
            [child for child in tree.children if isinstance(child, ServerLine)],
            tree.line
        )

    @lark.v_args(tree=True)
    def alternative_block(self, tree):
        return AlternativeBlock(
            [c for c in tree.children if not isinstance(c, lark.Token)],
            tree.line
        )

    @lark.v_args(tree=True)
    def optional_block(self, tree):
        return OptionalBlock(tree.children[2], tree.line)

    @lark.v_args(tree=True)
    def parallel_block(self, tree):
        return ParallelBlock(
            [c for c in tree.children if not isinstance(c, lark.Token)],
            tree.line
        )

    @lark.v_args(tree=True)
    def repeat_0_block(self, tree):
        return Repeat0Block(tree.children[2], tree.line)

    @lark.v_args(tree=True)
    def repeat_1_block(self, tree):
        return Repeat1Block(tree.children[2], tree.line)


def parse(script: str, substitutions: Optional[dict] = None) -> Script:
    if substitutions:
        for match, replacement in substitutions.items():
            script = script.replace(match, replacement)
    return ScriptTransformer().transform(parser.parse(script))


def parse_file(filename):
    with open(filename) as fd:
        script = parse(fd.read())
    script.filename = filename
    return script
