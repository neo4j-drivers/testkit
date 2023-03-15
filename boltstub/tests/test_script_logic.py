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


import contextlib
import itertools
from typing import Iterable

import pytest

from . import _common
from ..bolt_protocol import TranslatedStructure
from ..parsing import (
    AlternativeBlock,
    AutoBlock,
    AutoLine,
    BlockList,
    ClientBlock,
    ClientLine,
    ConditionalBlock,
    OptionalBlock,
    ParallelBlock,
    Repeat0Block,
    Repeat1Block,
    ServerBlock,
    ServerLine,
)
from ..simple_jolt import v1 as jolt_v1
from ..simple_jolt import v2 as jolt_v2
from ..util import EvalContext


class MockChannel:
    def __init__(self, messages: Iterable[TranslatedStructure],
                 packstream_version=1):
        self.messages = tuple(messages)
        self.index = 0
        self.raw_buffer = bytearray()
        self.msg_buffer = []
        self.packstream_version = packstream_version
        self.eval_context = EvalContext()
        if self.packstream_version == 1:
            self.jolt_package = jolt_v1
        elif self.packstream_version == 2:
            self.jolt_package = jolt_v2
        else:
            raise ValueError(
                f"Unknown packstream version: {packstream_version}"
            )

    def __repr__(self):
        return "MockChannel[{}][{}]".format(
            ", ".join(map(lambda m: m.name, self.messages[:self.index])),
            ", ".join(map(lambda m: m.name, self.messages[self.index:]))

        )

    def match_client_line(self, client_line, msg):
        # we only test using message names (no fields)
        client_line.parse_jolt(self.jolt_package)
        return client_line.match_message(msg.name, msg.fields)

    def send_raw(self, b):
        self.raw_buffer.extend(b)

    def send_struct(self, struct):
        self.msg_buffer.append(struct)

    def send_server_line(self, server_line):
        server_line.parse_jolt(self.jolt_package)
        name, fields = server_line.jolt_parsed
        msg = TranslatedStructure(name, b"\x00", *fields,
                                  packstream_version=self.packstream_version)
        self.msg_buffer.append(msg)

    def msg_buffer_names(self):
        return [msg.name for msg in self.msg_buffer]

    def consume(self, line_no):
        self.index += 1
        return self.messages[self.index - 1]

    def peek(self):
        return self.messages[self.index]

    def try_auto_consume(self, whitelist: Iterable[str]):
        return False

    def reset(self):
        self.index = 0

    @contextlib.contextmanager
    def assert_consume(self):
        prev_idx = self.index
        yield
        assert self.index == prev_idx + 1

    def auto_respond(self, msg):
        msg = TranslatedStructure("AUTO_REPLY", b"\x00", msg.name,
                                  packstream_version=1)
        self.msg_buffer.append(msg)


def channel_factory(msg_names, packstream_version=1):
    return MockChannel(
        [
            TranslatedStructure(
                n, b"\x00", packstream_version=packstream_version
            )
            for n in msg_names
        ],
        packstream_version=packstream_version
    )


def _test_block_reset_deterministic_end(block, channel, reset_idx):
    assert block.has_deterministic_end()
    for _ in range(reset_idx):
        assert not block.done(channel)
        with channel.assert_consume():
            assert block.try_consume(channel)
    block.reset()
    channel.reset()
    for _ in range(len(channel.messages) - 1):
        assert not block.done(channel)
        with channel.assert_consume():
            assert block.try_consume(channel)
    assert block.done(channel)


def _test_block_reset_nondeterministic_end(block, channel, reset_idx,
                                           skippable=None):
    def assert_skippability(step):
        if skippable is None:
            return
        if step in skippable:
            assert block.can_be_skipped(channel)
        else:
            assert not block.can_be_skipped(channel)

    assert not block.has_deterministic_end()
    for i in range(reset_idx):
        assert_skippability(i)
        with channel.assert_consume():
            assert block.try_consume(channel)
    block.reset()
    channel.reset()
    for step in range(len(channel.messages) - 1):
        assert_skippability(step)
        with channel.assert_consume():
            assert block.try_consume(channel)
    assert block.can_be_skipped(channel)


def _assert_accepted_messages(channel, block, messages):
    assert list(map(lambda line: line.content,
                    block.accepted_messages(channel))) == messages


def _assert_accepted_messages_after_reset(channel, block, messages):
    assert list(map(lambda line: line.content,
                    block.accepted_messages_after_reset(channel))) == messages


class TestAlternativeBlock:
    @pytest.fixture()
    def block_read(self):
        return AlternativeBlock([  # noqa: PAR101
            BlockList([ClientBlock([  # noqa: PAR101
                ClientLine(2, "C: MSG1", "MSG1")
            ], 2)], 2),
            BlockList([ClientBlock([  # noqa: PAR101
                ClientLine(3, "C: MSG2", "MSG2")
            ], 3)], 3),
        ], 1)

    @pytest.fixture()
    def block_write(self):
        return AlternativeBlock([  # noqa: PAR101
            BlockList([  # noqa: PAR101
                ClientBlock([  # noqa: PAR101
                    ClientLine(2, "C: MSG1", "MSG1"),
                ], 2),
                ServerBlock([  # noqa: PAR101
                    ServerLine(3, "S: SMSG1", "SMSG1")
                ], 3),
            ], 2),
            BlockList([  # noqa: PAR101
                ClientBlock([  # noqa: PAR101
                    ClientLine(4, "C: MSG2", "MSG2"),
                ], 4),
                ServerBlock([  # noqa: PAR101
                    ServerLine(5, "S: SMSG2", "SMSG2")
                ], 5),
            ], 5),
        ], 1)

    @pytest.fixture()
    def block_with_non_det_end(self):
        return AlternativeBlock([  # noqa: PAR101
            BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2),
                ServerBlock([ServerLine(3, "S: SMSG1", "SMSG1")], 3),
                OptionalBlock(BlockList([  # noqa: PAR101
                    ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                    ServerBlock([ServerLine(6, "S: SMSG2", "SMSG2")], 6)
                ], 5), 4),
            ], 2),
            BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(8, "C: MSG3", "MSG3")], 8),
                ServerBlock([ServerLine(9, "S: SMSG3", "SMSG3")], 9),
            ], 7)
        ], 1)

    @pytest.fixture()
    def block_with_non_det_block(self):
        return AlternativeBlock([  # noqa: PAR101
            BlockList([  # noqa: PAR101
                OptionalBlock(BlockList([  # noqa: PAR101
                    ClientBlock([ClientLine(3, "C: MSG1", "MSG1")], 3),
                    ServerBlock([ServerLine(4, "S: SMSG1", "SMSG1")], 4)
                ], 3), 2),
            ], 2),
            BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                ServerBlock([ServerLine(7, "S: SMSG2", "SMSG2")], 7),
            ], 5)
        ], 1)

    @pytest.mark.parametrize("branch", range(2))
    def test_block_read(self, block_read, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        branch_channel = (msg1_channel, msg2_channel)[branch]
        assert not block_read.done(branch_channel)
        assert block_read.can_consume(msg1_channel)
        assert block_read.can_consume(msg2_channel)
        _assert_accepted_messages(branch_channel, block_read,
                                  ["MSG1", "MSG2"])
        _assert_accepted_messages_after_reset(branch_channel, block_read,
                                              ["MSG1", "MSG2"])
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        assert block_read.done(branch_channel)
        assert not block_read.can_consume(msg1_channel)
        assert not block_read.can_consume(msg2_channel)
        branch_channel.reset()
        assert not block_read.can_consume(branch_channel)
        _assert_accepted_messages(branch_channel, block_read, [])
        _assert_accepted_messages_after_reset(branch_channel, block_read,
                                              ["MSG1", "MSG2"])

    @pytest.mark.parametrize("branch", range(2))
    def test_block_write(self, block_write, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        expected_message = ("SMSG1", "SMSG2")[branch]
        branch_channel = (msg1_channel, msg2_channel)[branch]
        _assert_accepted_messages(branch_channel, block_write,
                                  ["MSG1", "MSG2"])
        _assert_accepted_messages_after_reset(branch_channel, block_write,
                                              ["MSG1", "MSG2"])
        with branch_channel.assert_consume():
            assert block_write.try_consume(branch_channel)
        _assert_accepted_messages(branch_channel, block_write, [])
        _assert_accepted_messages_after_reset(branch_channel, block_write,
                                              ["MSG1", "MSG2"])
        assert branch_channel.msg_buffer_names() == [expected_message]
        assert block_write.done(branch_channel)
        assert not block_write.can_consume(msg1_channel)
        assert not block_write.can_consume(msg2_channel)
        branch_channel.reset()
        assert not block_write.can_consume(branch_channel)
        _assert_accepted_messages(branch_channel, block_write, [])
        _assert_accepted_messages_after_reset(branch_channel, block_write,
                                              ["MSG1", "MSG2"])

    @pytest.mark.parametrize("branch", range(2))
    def test_block_reset(self, block_read, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        branch_channel = (msg1_channel, msg2_channel)[branch]
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        block_read.reset()
        assert not block_read.done(branch_channel)
        branch_channel.reset()
        assert block_read.can_consume(msg1_channel)
        assert block_read.can_consume(msg2_channel)
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        assert block_read.done(branch_channel)

    @pytest.mark.parametrize("messages", (("1", "2"), ("1",), ("3",)))
    def test_non_det_ending_alternative(self, block_with_non_det_end,
                                        messages):
        channel = channel_factory(["MSG%s" % m for m in messages]
                                  + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_end.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_end.can_consume(channel)
        if block_with_non_det_end.has_deterministic_end():
            assert block_with_non_det_end.done(channel)
        else:
            assert block_with_non_det_end.can_be_skipped(channel)

    @pytest.mark.parametrize("messages", (("1",), ("2",), ()))
    def test_non_det_alternative(self, block_with_non_det_block, messages):
        channel = channel_factory(["MSG%s" % m for m in messages]
                                  + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_block.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_block.can_consume(channel)
        if block_with_non_det_block.has_deterministic_end():
            assert block_with_non_det_block.done(channel)
        else:
            assert block_with_non_det_block.can_be_skipped(channel)


class TestBlockList:
    @pytest.fixture()
    def block_det(self):
        return BlockList([  # noqa: PAR101
            ClientBlock([  # noqa: PAR101
                ClientLine(1, "C: MSG1", "MSG1"),
                ClientLine(2, "C: MSG2", "MSG2")
            ], 1),
            ServerBlock([  # noqa: PAR101
                ServerLine(3, "S: SMSG1", "SMSG1"),
                ServerLine(4, "S: SMSG2", "SMSG2")
            ], 3),
            ClientBlock([ClientLine(5, "C: MSG3", "MSG3")], 5),
        ], 1)

    @pytest.fixture()
    def block_non_det(self):
        return BlockList([  # noqa: PAR101
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2)
            ], 2), 1),
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5)
            ], 5), 4),
            ClientBlock([ClientLine(7, "C: MSG3", "MSG3")], 7),
        ], 1)

    @pytest.fixture()
    def block_only_non_det(self):
        return BlockList([  # noqa: PAR101
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2)
            ], 2), 1),
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5)
            ], 5), 4),
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(8, "C: MSG3", "MSG3")], 8)
            ], 8), 7),
        ], 1)

    @staticmethod
    def _skip_messages(messages, *skips):
        messages = [m for i, m in enumerate(messages)
                    if i >= len(skips) or not skips[i]]
        return messages

    @staticmethod
    def _skipping_channel(messages, *skips):
        return channel_factory(TestBlockList._skip_messages(messages, *skips))

    def test_consecutive_deterministic(self, block_det):
        messages = ["MSG1", "MSG2", "MSG3", "NOMATCH"]
        channel = channel_factory(messages)
        assert block_det.has_deterministic_end()
        assert not channel.msg_buffer
        for i in range(3):
            assert not block_det.done(channel)
            assert not block_det.can_be_skipped(channel)
            _assert_accepted_messages(channel, block_det, [messages[i]])
            _assert_accepted_messages_after_reset(channel, block_det,
                                                  [messages[0]])
            with channel.assert_consume():
                assert block_det.try_consume(channel)
            assert not block_det.can_consume(channel_factory(messages[i:]))
            if i < 1:
                assert not channel.msg_buffer
            else:
                assert channel.msg_buffer_names() == ["SMSG1", "SMSG2"]
        assert block_det.done(channel)
        assert block_det.can_be_skipped(channel)
        _assert_accepted_messages(channel, block_det, [])
        _assert_accepted_messages_after_reset(channel, block_det,
                                              [messages[0]])

    @pytest.mark.parametrize("skip1", (True, False))
    @pytest.mark.parametrize("skip2", (True, False))
    def test_consecutive_non_deterministic(self, skip1, skip2, block_non_det):
        all_messages = ["MSG1", "MSG2", "MSG3", "NOMATCH"]
        messages = TestBlockList._skip_messages(all_messages, skip1, skip2)
        channel = channel_factory(messages)
        assert block_non_det.has_deterministic_end()
        accepted = all_messages[:-1]
        for i in range(len(messages) - 1):
            assert not block_non_det.done(channel)
            assert not block_non_det.can_be_skipped(channel)
            _assert_accepted_messages(channel, block_non_det, accepted)
            _assert_accepted_messages_after_reset(channel, block_non_det,
                                                  all_messages[:-1])
            accepted = all_messages[(all_messages.index(messages[i]) + 1):-1]
            assert not block_non_det.done(channel)
            with channel.assert_consume():
                assert block_non_det.try_consume(channel)
            assert not block_non_det.can_consume(channel_factory(messages[i:]))
        assert block_non_det.can_be_skipped(channel)
        assert block_non_det.done(channel)
        _assert_accepted_messages(channel, block_non_det, [])
        _assert_accepted_messages_after_reset(channel, block_non_det,
                                              all_messages[:-1])

    @pytest.mark.parametrize(
        ("skip1", "skip2", "skip3"),
        set(itertools.product((True, False), repeat=3)) - {(True, True, True)}
    )
    def test_only_consecutive_non_deterministic(self, skip1, skip2, skip3,
                                                block_only_non_det):
        all_messages = ["MSG1", "MSG2", "MSG3", "NOMATCH"]
        messages = TestBlockList._skip_messages(all_messages,
                                                skip1, skip2, skip3)
        channel = channel_factory(messages)
        assert not block_only_non_det.has_deterministic_end()
        accepted = all_messages[:-1]
        for i in range(len(messages) - 1):
            _assert_accepted_messages(channel, block_only_non_det, accepted)
            _assert_accepted_messages_after_reset(channel, block_only_non_det,
                                                  all_messages[:-1])
            accepted = all_messages[(all_messages.index(messages[i]) + 1):-1]
            with pytest.raises(RuntimeError):
                assert not block_only_non_det.done(channel)
            assert block_only_non_det.can_be_skipped(channel)
            with channel.assert_consume():
                assert block_only_non_det.try_consume(channel)
            assert not block_only_non_det.can_consume(channel_factory(
                messages[i:]
            ))
        assert block_only_non_det.can_be_skipped(channel)
        _assert_accepted_messages(channel, block_only_non_det, accepted)
        _assert_accepted_messages_after_reset(channel, block_only_non_det,
                                              all_messages[:-1])
        if not skip3:
            # consumed last message => block should be done
            assert block_only_non_det.has_deterministic_end()
            assert block_only_non_det.done(channel)
        else:
            assert not block_only_non_det.has_deterministic_end()

    @pytest.mark.parametrize("reset_idx", range(1, 4))
    def test_reset_consecutive_deterministic(self, block_det, reset_idx):
        channel = channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])
        _test_block_reset_deterministic_end(block_det, channel, reset_idx)

    @pytest.mark.parametrize(
        ("skips", "reset_idx"),
        ((skips, reset_idx)
         for skips in (set(itertools.product((True, False), repeat=2))
                       - {(True, True)})
         for reset_idx in range(1, 4 - sum(skips)))
    )
    def test_reset_consecutive_non_deterministic(self, block_non_det,
                                                 skips, reset_idx):
        channel = TestBlockList._skipping_channel(
            ["MSG1", "MSG2", "MSG3", "NOMATCH"], *skips
        )
        _test_block_reset_deterministic_end(block_non_det, channel, reset_idx)

    @pytest.mark.parametrize(
        ("skips", "reset_idx"),
        ((skips, reset_idx)
         for skips in (set(itertools.product((True, False), repeat=3))
                       - {(True, True, True)})
         for reset_idx in range(1, 4 - sum(skips)))
    )
    def test_reset_only_consecutive_non_deterministic(self, block_only_non_det,
                                                      skips, reset_idx):
        channel = TestBlockList._skipping_channel(
            ["MSG1", "MSG2", "MSG3", "NOMATCH"], *skips
        )
        _test_block_reset_nondeterministic_end(block_only_non_det, channel,
                                               reset_idx, skippable={0, 1, 2})


class TestOptionalBlock:
    def test_block_cant_skip_out(self):
        block = OptionalBlock(BlockList([ClientBlock([  # noqa: PAR101
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        assert block.can_be_skipped(channel)

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert not block.can_be_skipped(channel)

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.can_be_skipped(channel)
        assert not block.can_consume(channel)

    @pytest.mark.parametrize("reset_idx", range(1, 2))
    def test_block_reset(self, reset_idx):
        block = OptionalBlock(BlockList([ClientBlock([  # noqa: PAR101
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        _test_block_reset_nondeterministic_end(block, channel, reset_idx,
                                               skippable={0})

    def test_block_can_skip_optional_end(self):
        block = OptionalBlock(BlockList([  # noqa: PAR101
            ClientBlock([  # noqa: PAR101
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2")
            ], 2),
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([  # noqa: PAR101
                    ClientLine(5, "C: MSG3", "MSG3")
                ], 5)
            ], 5), 4)
        ], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])
        assert block.can_be_skipped(channel)

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert not block.can_be_skipped(channel)

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.can_be_skipped(channel)

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.can_be_skipped(channel)
        assert not block.can_consume(channel)

    def test_becomes_deterministic_when_started(self):
        block = OptionalBlock(BlockList([ClientBlock([  # noqa: PAR101
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        assert not block.has_deterministic_end()
        assert block.try_consume(channel)
        assert block.has_deterministic_end()
        assert not block.done(channel)
        assert block.try_consume(channel)
        assert block.has_deterministic_end()
        assert block.done(channel)

    def test_accepted_messages(self):
        block = OptionalBlock(BlockList([ClientBlock([  # noqa: PAR101
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        _assert_accepted_messages(channel, block, ["MSG1"])
        _assert_accepted_messages_after_reset(channel, block, ["MSG1"])
        assert block.try_consume(channel)
        _assert_accepted_messages(channel, block, ["MSG2"])
        _assert_accepted_messages_after_reset(channel, block, ["MSG1"])
        assert block.try_consume(channel)
        _assert_accepted_messages(channel, block, [])
        _assert_accepted_messages_after_reset(channel, block, ["MSG1"])


class TestParallelBlock:
    @pytest.fixture()
    def block_read(self):
        return ParallelBlock([  # noqa: PAR101
            BlockList([ClientBlock([  # noqa: PAR101
                ClientLine(2, "C: MSG11", "MSG11"),
                ClientLine(2, "C: MSG12", "MSG12")
            ], 2)], 2),
            BlockList([ClientBlock([  # noqa: PAR101
                ClientLine(3, "C: MSG21", "MSG21"),
                ClientLine(3, "C: MSG21", "MSG22")
            ], 3)], 3),
        ], 1)

    @pytest.fixture()
    def block_write(self):
        return ParallelBlock([  # noqa: PAR101
            BlockList([  # noqa: PAR101
                ClientBlock([  # noqa: PAR101
                    ClientLine(2, "C: MSG1", "MSG1"),
                ], 2),
                ServerBlock([  # noqa: PAR101
                    ServerLine(3, "S: SMSG1", "SMSG1")
                ], 3),
            ], 2),
            BlockList([  # noqa: PAR101
                ClientBlock([  # noqa: PAR101
                    ClientLine(4, "C: MSG2", "MSG2"),
                ], 4),
                ServerBlock([  # noqa: PAR101
                    ServerLine(5, "S: SMSG2", "SMSG2")
                ], 5),
            ], 5),
        ], 1)

    @pytest.fixture()
    def block_with_non_det_end(self):
        return ParallelBlock([  # noqa: PAR101
            BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2),
                ServerBlock([ServerLine(3, "S: SMSG1", "SMSG1")], 3),
                OptionalBlock(BlockList([  # noqa: PAR101
                    ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                    ServerBlock([ServerLine(6, "S: SMSG2", "SMSG2")], 6)
                ], 5), 4),
            ], 2),
            BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(8, "C: MSG3", "MSG3")], 8),
                ServerBlock([ServerLine(9, "S: SMSG3", "SMSG3")], 9),
            ], 7)
        ], 1)

    @pytest.fixture()
    def block_with_non_det_block(self):
        return ParallelBlock([  # noqa: PAR101
            BlockList([  # noqa: PAR101
                OptionalBlock(BlockList([  # noqa: PAR101
                    ClientBlock([ClientLine(3, "C: MSG1", "MSG1")], 3),
                    ServerBlock([ServerLine(4, "S: SMSG1", "SMSG1")], 4)
                ], 3), 2),
            ], 2),
            BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                ServerBlock([ServerLine(7, "S: SMSG2", "SMSG2")], 7),
            ], 5)
        ], 1)

    @pytest.mark.parametrize("order", itertools.permutations((0, 0, 1, 1), 4))
    def test_block_read(self, block_read, order):
        msg1 = ["MSG11", "MSG12"]
        msg2 = ["MSG21", "MSG22"]
        idxs = [0, 0]
        messages = [(msg1, msg2)[idx].pop(0) for idx in order] + ["NOMATCH"]
        msg1 = ["MSG11", "MSG12"]
        msg2 = ["MSG21", "MSG22"]
        channel = channel_factory(messages)
        for i in range(4):
            accepted = (msg1[idxs[0]:(idxs[0] + 1)]
                        + msg2[idxs[1]:(idxs[1] + 1)])
            _assert_accepted_messages(channel, block_read, accepted)
            _assert_accepted_messages_after_reset(channel, block_read,
                                                  [msg1[0], msg2[0]])
            idxs[order[i]] += 1
            assert not block_read.done(channel)
            with channel.assert_consume():
                assert block_read.try_consume(channel)
        _assert_accepted_messages(channel, block_read, [])
        _assert_accepted_messages_after_reset(channel, block_read,
                                              [msg1[0], msg2[0]])
        assert block_read.done(channel)
        channel.reset()
        assert not block_read.can_consume(channel)

    @pytest.mark.parametrize("order", itertools.permutations((0, 1), 2))
    def test_block_write(self, block_write, order):
        if order == (0, 1):
            channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
            expected_sends = ["SMSG1", "SMSG2"]
        else:
            channel = channel_factory(["MSG2", "MSG1", "NOMATCH"])
            expected_sends = ["SMSG2", "SMSG1"]

        for i in range(2):
            assert len(channel.msg_buffer) == i
            assert not block_write.done(channel)
            with channel.assert_consume():
                assert block_write.try_consume(channel)
        assert block_write.done(channel)
        assert channel.msg_buffer_names() == expected_sends
        channel.reset()
        assert block_write.done(channel)
        assert not block_write.can_consume(channel)

    @pytest.mark.parametrize("order", itertools.permutations((0, 0, 1, 1), 4))
    @pytest.mark.parametrize("reset_idx", range(1, 5))
    def test_block_reset(self, block_read, order, reset_idx):
        msg1 = ["MSG11", "MSG12"]
        msg2 = ["MSG21", "MSG22"]
        messages = [(msg1, msg2)[idx].pop(0) for idx in order] + ["NOMATCH"]
        channel = channel_factory(messages)
        _test_block_reset_deterministic_end(block_read, channel, reset_idx)

    @pytest.mark.parametrize("messages", (
        ("1", "2", "3"), ("1", "3"),
    ))
    def test_non_det_ending_alternative(self, block_with_non_det_end,
                                        messages):
        channel = channel_factory(["MSG%s" % m for m in messages]
                                  + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_end.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_end.can_consume(channel)
        if block_with_non_det_end.has_deterministic_end():
            assert block_with_non_det_end.done(channel)
        else:
            assert block_with_non_det_end.can_be_skipped(channel)

    @pytest.mark.parametrize("messages", (
        ("1", "2"), ("2",),
    ))
    def test_non_det_alternative(self, block_with_non_det_block, messages):
        channel = channel_factory(["MSG%s" % m for m in messages]
                                  + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_block.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_block.can_consume(channel)
        if block_with_non_det_block.has_deterministic_end():
            assert block_with_non_det_block.done(channel)
        else:
            assert block_with_non_det_block.can_be_skipped(channel)


class TestClientBlock:
    @pytest.fixture()
    def single_block(self):
        return ClientBlock([  # noqa: PAR101
            ClientLine(1, "C: MSG1", "MSG1"),
        ], 1)

    @pytest.fixture()
    def single_channel(self):
        return channel_factory(["MSG1", "NOMATCH"])

    @pytest.fixture()
    def multi_block(self):
        return ClientBlock([  # noqa: PAR101
            ClientLine(1, "C: MSG1", "MSG1"),
            ClientLine(2, "C: MSG2", "MSG2"),
            ClientLine(3, "C: MSG3", "MSG3"),
        ], 1)

    @pytest.fixture()
    def multi_channel(self):
        return channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])

    def test_single_block(self, single_block, single_channel):
        assert single_block.can_consume(single_channel)
        assert not single_block.done(single_channel)
        assert not single_channel.msg_buffer
        assert single_block.can_consume(single_channel)
        _assert_accepted_messages(single_channel, single_block, ["MSG1"])
        _assert_accepted_messages_after_reset(single_channel, single_block,
                                              ["MSG1"])
        single_block.init(single_channel)
        assert not single_block.done(single_channel)
        assert not single_channel.msg_buffer
        _assert_accepted_messages(single_channel, single_block, ["MSG1"])
        _assert_accepted_messages_after_reset(single_channel, single_block,
                                              ["MSG1"])
        assert single_block.can_consume(single_channel)
        with single_channel.assert_consume():
            assert single_block.try_consume(single_channel)
        _assert_accepted_messages(single_channel, single_block, [])
        _assert_accepted_messages_after_reset(single_channel, single_block,
                                              ["MSG1"])
        assert not single_channel.msg_buffer
        assert single_block.done(single_channel)
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)

    def test_multi_block(self, multi_block, multi_channel):
        assert multi_block.can_consume(multi_channel)
        assert not multi_block.done(multi_channel)
        assert not multi_channel.msg_buffer
        _assert_accepted_messages(multi_channel, multi_block, ["MSG1"])
        _assert_accepted_messages_after_reset(multi_channel, multi_block,
                                              ["MSG1"])
        multi_block.init(multi_channel)
        assert not multi_block.done(multi_channel)
        assert not multi_channel.msg_buffer
        for i in range(3):
            _assert_accepted_messages(multi_channel, multi_block,
                                      ["MSG1", "MSG2", "MSG3"][i:(i + 1)])
            _assert_accepted_messages_after_reset(multi_channel, multi_block,
                                                  ["MSG1"])
            assert multi_block.can_consume(multi_channel)
            with multi_channel.assert_consume():
                assert multi_block.try_consume(multi_channel)
        _assert_accepted_messages(multi_channel, multi_block, [])
        _assert_accepted_messages_after_reset(multi_channel, multi_block,
                                              ["MSG1"])
        assert not multi_channel.msg_buffer
        assert multi_block.done(multi_channel)
        assert not multi_block.can_consume(multi_channel)
        assert not multi_block.try_consume(multi_channel)


class TestAutoBlock:
    @pytest.fixture()
    def single_block(self):
        return AutoBlock(AutoLine(1, "A: MSG1", "MSG1"), 1)

    @pytest.fixture()
    def single_channel(self):
        return channel_factory(["MSG1", "NOMATCH"])

    def test_single_block(self, single_block, single_channel):
        assert single_block.can_consume(single_channel)
        assert not single_block.done(single_channel)
        assert not single_channel.msg_buffer
        assert single_block.can_consume(single_channel)
        _assert_accepted_messages(single_channel, single_block, ["MSG1"])
        _assert_accepted_messages_after_reset(single_channel, single_block,
                                              ["MSG1"])
        single_block.init(single_channel)
        assert not single_block.done(single_channel)
        assert not single_channel.msg_buffer
        _assert_accepted_messages(single_channel, single_block, ["MSG1"])
        _assert_accepted_messages_after_reset(single_channel, single_block,
                                              ["MSG1"])
        assert single_block.can_consume(single_channel)
        with single_channel.assert_consume():
            assert single_block.try_consume(single_channel)
        _assert_accepted_messages(single_channel, single_block, [])
        _assert_accepted_messages_after_reset(single_channel, single_block,
                                              ["MSG1"])
        assert single_block.done(single_channel)
        assert len(single_channel.msg_buffer) == 1
        msg = single_channel.msg_buffer[0]
        assert isinstance(msg, TranslatedStructure)
        assert msg.name == "AUTO_REPLY"
        assert msg.fields == ["MSG1"]
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)


class TestServerBlock:
    @pytest.fixture()
    def single_block(self):
        return ServerBlock([  # noqa: PAR101
            ServerLine(1, "S: SMSG1", "SMSG1"),
        ], 1)

    @pytest.fixture()
    def single_channel(self):
        return channel_factory(["SMSG1", "NOMATCH"])

    @pytest.fixture()
    def multi_block(self):
        return ServerBlock([  # noqa: PAR101
            ServerLine(1, "S: SMSG1", "SMSG1"),
            ServerLine(2, "S: SMSG2", "SMSG2"),
            ServerLine(3, "S: SMSG3", "SMSG3"),
        ], 1)

    @pytest.fixture()
    def multi_channel(self):
        return channel_factory(["NOMATCH"])

    @pytest.mark.parametrize(("content", "fields", "packstream_version"), (
        *_common.JOLT_FIELD_REPR_TO_FIELDS,
    ))
    def test_send_jolt(self, content, fields, packstream_version):
        channel = channel_factory(["NOMATCH"],
                                  packstream_version=packstream_version)
        content = "SMSG1 " + content
        block = ServerBlock([  # noqa: PAR101
            ServerLine(1, "S: " + content, content)
        ], 1)
        block.init(channel)
        assert len(channel.msg_buffer) == 1
        msg = channel.msg_buffer[0]
        assert isinstance(msg, TranslatedStructure)
        assert msg.name == "SMSG1"
        assert _common.nan_and_type_equal(msg.fields, fields)

    def test_single_block(self, single_block, single_channel):
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)
        assert not single_block.done(single_channel)
        assert not single_channel.msg_buffer
        _assert_accepted_messages(single_channel, single_block, [])
        _assert_accepted_messages_after_reset(single_channel, single_block, [])
        single_block.init(single_channel)
        _assert_accepted_messages(single_channel, single_block, [])
        _assert_accepted_messages_after_reset(single_channel, single_block, [])
        assert single_block.done(single_channel)
        assert single_channel.msg_buffer_names() == ["SMSG1"]
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)

    def test_multi_block(self, multi_block, multi_channel):
        assert not multi_block.can_consume(multi_channel)
        assert not multi_block.try_consume(multi_channel)
        assert not multi_block.done(multi_channel)
        assert not multi_channel.msg_buffer
        _assert_accepted_messages(multi_channel, multi_block, [])
        _assert_accepted_messages_after_reset(multi_channel, multi_block, [])
        multi_block.init(multi_channel)
        _assert_accepted_messages(multi_channel, multi_block, [])
        _assert_accepted_messages_after_reset(multi_channel, multi_block, [])
        assert multi_block.done(multi_channel)
        assert multi_channel.msg_buffer_names() == ["SMSG1", "SMSG2", "SMSG3"]
        assert not multi_block.can_consume(multi_channel)
        assert not multi_block.try_consume(multi_channel)


class _TestRepeatBlock:
    must_run_once = None
    block_cls = None

    @pytest.fixture()
    def block_1(self):
        return self.block_cls(BlockList([  # noqa: PAR101
            ClientBlock([  # noqa: PAR101
                ClientLine(2, "C: MSG1", "MSG1"),
            ], 2)
        ], 2), 1)

    @pytest.fixture()
    def channel_1(self):
        return channel_factory(["MSG1", "NOMATCH"])

    @pytest.fixture()
    def block_2(self):
        return self.block_cls(BlockList([  # noqa: PAR101
            ClientBlock([  # noqa: PAR101
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2")
            ], 2)
        ], 2), 1)

    @pytest.fixture()
    def channel_2(self):
        return channel_factory(["MSG1", "MSG2", "NOMATCH"])

    @pytest.fixture()
    def block_optional_end(self):
        return self.block_cls(BlockList([  # noqa: PAR101
            ClientBlock([  # noqa: PAR101
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2"),
            ], 2),
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(5, "C: MSG3", "MSG3")], 5)
            ], 5), 4),
        ], 2), 1)

    @pytest.fixture()
    def channel_optional_end(self):
        return channel_factory(["MSG1", "MSG2",  "MSG3", "NOMATCH"])

    @pytest.fixture()
    def block_all_optional(self):
        return self.block_cls(BlockList([  # noqa: PAR101
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(3, "C: MSG1", "MSG1")], 3)
            ], 3), 2),
            OptionalBlock(BlockList([  # noqa: PAR101
                ClientBlock([ClientLine(6, "C: MSG2", "MSG2")], 6)
            ], 6), 5),
        ], 2), 1)

    @pytest.fixture()
    def channel_all_optional(self):
        return channel_factory(["MSG1", "MSG2", "NOMATCH"])

    def _accepted_messages(self, channel: MockChannel, expected_idx,
                           skippable):
        messages = [m.name for m in channel.messages if m.name != "NOMATCH"]
        assert messages
        expected_idx = expected_idx % len(messages)
        accepted = [expected_idx] if expected_idx < len(messages) else []
        for i in range(expected_idx, len(messages) * 2):
            if i == len(messages):
                if 0 not in accepted:
                    accepted.append(0)
            if i % len(messages) in skippable:
                if (i + 1) % len(messages) not in accepted:
                    accepted.append((i + 1) % len(messages))
            else:
                return [messages[i_] for i_ in accepted]
        return [messages[i_] for i_ in accepted]

    def _test_loop_run(self, block, channel, skippable=None, skip=None):
        assert len(channel.messages) >= 1
        expected_idx = 0
        if skippable is None:
            skippable = set()
        if skip is None:
            skip = set()
        if 0 in skip:
            raise ValueError("Test code is not written for this.")
        for run in range(2):
            _assert_accepted_messages(
                channel, block,
                self._accepted_messages(channel, expected_idx, skippable)
            )
            _assert_accepted_messages_after_reset(
                channel, block, self._accepted_messages(channel, 0, skippable)
            )
            if run == 0 and self.must_run_once and 0 not in skippable:
                assert not block.can_be_skipped(channel)
            else:
                assert block.can_be_skipped(channel)
            with channel.assert_consume():
                assert block.try_consume(channel)
            expected_idx = 1
            for step in range(1, len(channel.messages) - 1):
                _assert_accepted_messages(
                    channel, block,
                    self._accepted_messages(channel, expected_idx, skippable)
                )
                _assert_accepted_messages_after_reset(
                    channel, block,
                    self._accepted_messages(channel, 0, skippable)
                )
                if step in skippable:
                    assert block.can_be_skipped(channel)
                else:
                    assert not block.can_be_skipped(channel)
                if step in skip:
                    channel.consume(None)
                    continue
                with channel.assert_consume():
                    assert block.try_consume(channel)
                expected_idx += 1
            assert block.can_be_skipped(channel)
            _assert_accepted_messages(
                channel, block,
                self._accepted_messages(channel, expected_idx, skippable)
            )
            _assert_accepted_messages_after_reset(
                channel, block, self._accepted_messages(channel, 0, skippable)
            )
            assert block.can_be_skipped(channel)
            channel.reset()

    def _test_loop_run_with_skip(self, block, channel, skip_idx):
        assert len(channel.messages) >= 1
        for _run in range(2):
            with channel.assert_consume():
                assert block.try_consume(channel)
            for _step in range(1, skip_idx):
                with channel.assert_consume():
                    assert block.try_consume(channel)
            assert block.can_be_skipped(channel)
            channel.reset()

    def test_loop_1(self, block_1, channel_1):
        self._test_loop_run(block_1, channel_1)

    def test_loop_2(self, block_2, channel_2):
        self._test_loop_run(block_2, channel_2)

    def test_loop_optional_end(self, block_optional_end, channel_optional_end):
        self._test_loop_run(block_optional_end, channel_optional_end,
                            skippable={2})

    def test_loop_optional_end_skip(self, block_optional_end,
                                    channel_optional_end):
        self._test_loop_run(block_optional_end, channel_optional_end,
                            skippable={2}, skip={2})

    @pytest.mark.parametrize("skip_idx", (2,))
    def test_loop_skip_optional_end(self, block_optional_end,
                                    channel_optional_end, skip_idx):
        self._test_loop_run_with_skip(block_optional_end, channel_optional_end,
                                      skip_idx)

    def test_loop_all_optional(self, block_all_optional, channel_all_optional):
        self._test_loop_run(block_all_optional, channel_all_optional,
                            skippable={0, 1})

    @pytest.mark.parametrize("skip_idx", (0, 1))
    def test_loop_skip_all_optional(self, block_all_optional,
                                    channel_all_optional, skip_idx):
        self._test_loop_run_with_skip(block_all_optional, channel_all_optional,
                                      skip_idx)

    def test_cant_skip_too_soon(self, block_optional_end,
                                channel_optional_end):
        if self.must_run_once:
            assert not block_optional_end.can_be_skipped(channel_optional_end)
        else:
            assert block_optional_end.can_be_skipped(channel_optional_end)
        assert block_optional_end.try_consume(channel_optional_end)
        assert not block_optional_end.can_be_skipped(channel_optional_end)
        channel_optional_end.reset()
        assert not block_optional_end.can_be_skipped(channel_optional_end)
        assert not block_optional_end.try_consume(channel_optional_end)
        assert not block_optional_end.can_be_skipped(channel_optional_end)


class TestRepeat0Block(_TestRepeatBlock):
    must_run_once = False
    block_cls = Repeat0Block


class TestRepeat1Block(_TestRepeatBlock):
    must_run_once = True
    block_cls = Repeat1Block


class TestConditionalBlock:
    @pytest.fixture()
    def block(self):
        return ConditionalBlock(
            ["if_", "elif_"],
            [
                ClientBlock([  # noqa: PAR101
                    ClientLine(2, "C: MSG_IF_1", "MSG_IF_1"),
                    ClientLine(3, "C: MSG_IF_2", "MSG_IF_2"),
                ], 2),
                ClientBlock([  # noqa: PAR101
                    ClientLine(5, "C: MSG_ELIF_1", "MSG_ELIF_1"),
                    ClientLine(6, "C: MSG_ELIF_2", "MSG_ELIF_2"),
                ], 5),
            ],
            1
        )

    @pytest.fixture()
    def block_else(self):
        return ConditionalBlock(
            ["if_"],
            [
                ClientBlock([  # noqa: PAR101
                    ClientLine(2, "C: MSG_IF_1", "MSG_IF_1"),
                    ClientLine(3, "C: MSG_IF_2", "MSG_IF_2"),
                ], 2),
                ClientBlock([  # noqa: PAR101
                    ClientLine(5, "C: MSG_ELSE_1", "MSG_ELSE_1"),
                    ClientLine(6, "C: MSG_ELSE_2", "MSG_ELSE_2"),
                ], 5),
            ],
            1
        )

    @pytest.fixture()
    def channel_if(self):
        channel = channel_factory(["MSG_IF_1", "MSG_IF_2", "NOMATCH"])
        channel.eval_context["if_"] = True
        return channel

    @pytest.fixture()
    def channel_elif(self):
        channel = channel_factory(["MSG_ELIF_1", "MSG_ELIF_2", "NOMATCH"])
        channel.eval_context["if_"] = False
        channel.eval_context["elif_"] = True
        return channel

    @pytest.fixture()
    def channel_else(self):
        channel = channel_factory(["MSG_ELSE_1", "MSG_ELSE_2", "NOMATCH"])
        channel.eval_context["if_"] = False
        return channel

    @pytest.fixture()
    def channel_none(self):
        channel = channel_factory(["NOMATCH"])
        channel.eval_context["if_"] = False
        channel.eval_context["elif_"] = False
        return channel

    def test_if_branch(self, block, channel_if):
        assert not block.can_be_skipped(channel_if)
        assert not block.done(channel_if)
        _assert_accepted_messages(channel_if, block, ["MSG_IF_1"])
        _assert_accepted_messages_after_reset(channel_if, block, ["MSG_IF_1"])
        assert block.try_consume(channel_if)
        assert not block.can_be_skipped(channel_if)
        assert not block.done(channel_if)
        _assert_accepted_messages(channel_if, block, ["MSG_IF_2"])
        _assert_accepted_messages_after_reset(channel_if, block, ["MSG_IF_1"])
        assert block.try_consume(channel_if)
        assert block.done(channel_if)
        assert not block.try_consume(channel_if)

    def test_elif_branch(self, block, channel_elif):
        assert not block.can_be_skipped(channel_elif)
        assert not block.done(channel_elif)
        _assert_accepted_messages(channel_elif, block, ["MSG_ELIF_1"])
        _assert_accepted_messages_after_reset(channel_elif, block,
                                              ["MSG_ELIF_1"])
        assert block.try_consume(channel_elif)
        assert not block.can_be_skipped(channel_elif)
        assert not block.done(channel_elif)
        _assert_accepted_messages(channel_elif, block, ["MSG_ELIF_2"])
        _assert_accepted_messages_after_reset(channel_elif, block,
                                              ["MSG_ELIF_1"])
        assert block.try_consume(channel_elif)
        assert block.done(channel_elif)
        assert not block.try_consume(channel_elif)

    def test_else_branch(self, block_else, channel_else):
        assert not block_else.can_be_skipped(channel_else)
        assert not block_else.done(channel_else)
        _assert_accepted_messages(channel_else, block_else, ["MSG_ELSE_1"])
        _assert_accepted_messages_after_reset(channel_else, block_else,
                                              ["MSG_ELSE_1"])
        assert block_else.try_consume(channel_else)
        assert not block_else.can_be_skipped(channel_else)
        assert not block_else.done(channel_else)
        _assert_accepted_messages(channel_else, block_else, ["MSG_ELSE_2"])
        _assert_accepted_messages_after_reset(channel_else, block_else,
                                              ["MSG_ELSE_1"])
        assert block_else.try_consume(channel_else)
        assert block_else.done(channel_else)
        assert not block_else.try_consume(channel_else)

    def test_no_branch(self, block, channel_none):
        assert block.can_be_skipped(channel_none)
        assert block.done(channel_none)
        _assert_accepted_messages(channel_none, block, [])
        _assert_accepted_messages_after_reset(channel_none, block, [])
        assert not block.try_consume(channel_none)
