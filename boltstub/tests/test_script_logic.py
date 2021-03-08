import contextlib
import itertools
import pytest
from typing import Iterable

from ..bolt_protocol import TranslatedStructure
from ..parsing import (
    ClientLine,
    ServerLine,
    AlternativeBlock,
    BlockList,
    OptionalBlock,
    ParallelBlock,
    parse_line,
    PlainBlock,
    Repeat0Block,
    Repeat1Block,
)


class MockChannel:
    def __init__(self, messages: Iterable[TranslatedStructure]):
        self.messages = tuple(messages)
        self._index = 0
        self.raw_buffer = bytearray()
        self.msg_buffer = []

    def __repr__(self):
        return "MockChannel[{}][{}]".format(
            ", ".join(map(lambda m: m.name, self.messages[:self._index])),
            ", ".join(map(lambda m: m.name, self.messages[self._index:]))

        )

    def send_raw(self, b):
        self.raw_buffer.extend(b)

    def send_struct(self, struct):
        self.msg_buffer.append(struct)

    def send_server_line(self, server_line):
        msg = TranslatedStructure(*parse_line(server_line.content))
        self.msg_buffer.append(msg)

    def msg_buffer_names(self):
        return [msg.name for msg in self.msg_buffer]

    def consume(self):
        self._index += 1
        return self.messages[self._index - 1]

    def peek(self):
        return self.messages[self._index]

    def try_auto_consume(self, whitelist: Iterable[str]):
        return False

    def reset(self):
        self._index = 0

    @contextlib.contextmanager
    def assert_consume(self):
        prev_idx = self._index
        yield
        assert self._index == prev_idx + 1


def channel_factory(msg_names):
    return MockChannel([TranslatedStructure(n, b"\x00") for n in msg_names])


def _test_block_reset_deterministic_end(block, channel, reset_idx):
    assert block.has_deterministic_end()
    for _ in range(reset_idx):
        assert not block.done()
        with channel.assert_consume():
            assert block.try_consume(channel)
    block.reset()
    channel.reset()
    for _ in range(len(channel.messages) - 1):
        assert not block.done()
        with channel.assert_consume():
            assert block.try_consume(channel)
    assert block.done()


def _test_block_reset_nondeterministic_end(block, channel, reset_idx,
                                           skippable=None):
    def assert_skippability(step):
        if skippable is None:
            return
        if step in skippable:
            assert block.can_be_skipped()
        else:
            assert not block.can_be_skipped()

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
    assert block.can_be_skipped()


class TestAlternativeBlock:
    @pytest.fixture()
    def block_read(self):
        return AlternativeBlock(
            [
                BlockList([PlainBlock([
                    ClientLine(2, "C: MSG1", "MSG1")
                ], 2)], 2),
                BlockList([PlainBlock([
                    ClientLine(3, "C: MSG2", "MSG2")
                ], 3)], 3),
            ], 1
        )

    @pytest.fixture()
    def block_write(self):
        return AlternativeBlock(
            [
                BlockList([PlainBlock([
                    ClientLine(2, "C: MSG1", "MSG1"),
                    ServerLine(3, "S: SMSG1", "SMSG1")
                ], 2)], 2),
                BlockList([PlainBlock([
                    ClientLine(4, "C: MSG2", "MSG2"),
                    ServerLine(5, "S: SMSG2", "SMSG2")
                ], 4)], 5),
            ], 1
        )

    @pytest.mark.parametrize("branch", range(2))
    def test_block_read(self, block_read, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        branch_channel = (msg1_channel, msg2_channel)[branch]
        assert not block_read.done()
        assert block_read.can_consume(msg1_channel)
        assert block_read.can_consume(msg2_channel)
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        assert block_read.done()
        assert not block_read.can_consume(msg1_channel)
        assert not block_read.can_consume(msg2_channel)
        branch_channel.reset()
        assert not block_read.can_consume(branch_channel)

    @pytest.mark.parametrize("branch", range(2))
    def test_block_write(self, block_write, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        expected_message = ("SMSG1", "SMSG2")[branch]
        branch_channel = (msg1_channel, msg2_channel)[branch]
        with branch_channel.assert_consume():
            assert block_write.try_consume(branch_channel)
        assert branch_channel.msg_buffer_names() == [expected_message]
        assert block_write.done()
        assert not block_write.can_consume(msg1_channel)
        assert not block_write.can_consume(msg2_channel)
        branch_channel.reset()
        assert not block_write.can_consume(branch_channel)

    @pytest.mark.parametrize("branch", range(2))
    def test_block_reset(self, block_read, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        branch_channel = (msg1_channel, msg2_channel)[branch]
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        block_read.reset()
        assert not block_read.done()
        branch_channel.reset()
        assert block_read.can_consume(msg1_channel)
        assert block_read.can_consume(msg2_channel)
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        assert block_read.done()


class TestBlockList:
    @pytest.fixture()
    def block_det(self):
        return BlockList([
            PlainBlock([
                ClientLine(1, "C: MSG1", "MSG1"),
                ClientLine(2, "C: MSG2", "MSG2")
            ], 1),
            PlainBlock([ClientLine(3, "C: MSG3", "MSG3")], 3),
        ], 1)

    @pytest.fixture()
    def block_non_det(self):
        return BlockList([
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(2, "C: MSG1", "MSG1")], 2)
            ], 2), 1),
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(5, "C: MSG2", "MSG2")], 5)
            ], 5), 4),
            PlainBlock([ClientLine(7, "C: MSG3", "MSG3")], 7),
        ], 1)

    @pytest.fixture()
    def block_only_non_det(self):
        return BlockList([
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(2, "C: MSG1", "MSG1")], 2)
            ], 2), 1),
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(5, "C: MSG2", "MSG2")], 5)
            ], 5), 4),
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(8, "C: MSG3", "MSG3")], 8)
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
        for i in range(3):
            assert not block_det.done()
            with channel.assert_consume():
                assert block_det.try_consume(channel)
            assert not block_det.can_consume(channel_factory(messages[i:]))
        assert block_det.done()

    @pytest.mark.parametrize("skip1", (True, False))
    @pytest.mark.parametrize("skip2", (True, False))
    def test_consecutive_non_deterministic(self, skip1, skip2, block_non_det):
        messages = TestBlockList._skip_messages(
            ["MSG1", "MSG2", "MSG3", "NOMATCH"], skip1, skip2
        )
        channel = channel_factory(messages)
        assert block_non_det.has_deterministic_end()
        for i in range(len(messages) - 1):
            assert not block_non_det.done()
            with channel.assert_consume():
                assert block_non_det.try_consume(channel)
            assert not block_non_det.can_consume(channel_factory(messages[i:]))
        assert block_non_det.done()

    @pytest.mark.parametrize(
        ("skip1", "skip2", "skip3"),
        set(itertools.product((True, False), repeat=3)) - {(True, True, True)}
    )
    def test_only_consecutive_non_deterministic(self, skip1, skip2, skip3,
                                                block_only_non_det):
        messages = TestBlockList._skip_messages(
            ["MSG1", "MSG2", "MSG3", "NOMATCH"], skip1, skip2, skip3
        )
        channel = channel_factory(messages)
        assert not block_only_non_det.has_deterministic_end()
        for i in range(len(messages) - 1):
            with pytest.raises(Exception):
                assert not block_only_non_det.done()
            with channel.assert_consume():
                assert block_only_non_det.try_consume(channel)
            assert not block_only_non_det.can_consume(channel_factory(
                messages[i:]
            ))

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
        block = OptionalBlock(BlockList([PlainBlock([
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        assert block.can_be_skipped()

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert not block.can_be_skipped()

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.can_be_skipped()
        assert not block.can_consume(channel)

    @pytest.mark.parametrize("reset_idx", range(1, 2))
    def test_block_reset(self, reset_idx):
        block = OptionalBlock(BlockList([PlainBlock([
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        _test_block_reset_nondeterministic_end(block, channel, reset_idx,
                                               skippable={0})

    def test_block_can_skip_optional_end(self):
        block = OptionalBlock(BlockList([
            PlainBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2")
            ], 2),
            OptionalBlock(BlockList([
                PlainBlock([
                    ClientLine(5, "C: MSG3", "MSG3")
                ], 5)
            ], 5), 4)
        ], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])
        assert block.can_be_skipped()

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert not block.can_be_skipped()

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.can_be_skipped()

        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.can_be_skipped()
        assert not block.can_consume(channel)

    def test_becomes_deterministic_when_started(self):
        pass


class TestParallelBlock:
    @pytest.fixture()
    def block_read(self):
        return ParallelBlock(
            [
                BlockList([PlainBlock([
                    ClientLine(2, "C: MSG11", "MSG11"),
                    ClientLine(2, "C: MSG12", "MSG12")
                ], 2)], 2),
                BlockList([PlainBlock([
                    ClientLine(3, "C: MSG21", "MSG21"),
                    ClientLine(3, "C: MSG21", "MSG22")
                ], 3)], 3),
            ], 1
        )

    @pytest.fixture()
    def block_write(self):
        return ParallelBlock(
            [
                BlockList([PlainBlock([
                    ClientLine(2, "C: MSG1", "MSG1"),
                    ServerLine(3, "S: SMSG1", "SMSG1")
                ], 2)], 2),
                BlockList([PlainBlock([
                    ClientLine(4, "C: MSG2", "MSG2"),
                    ServerLine(5, "S: SMSG2", "SMSG2")
                ], 4)], 5),
            ], 1
        )

    @pytest.mark.parametrize("order", itertools.permutations((0, 0, 1, 1), 4))
    def test_block_read(self, block_read, order):
        msg1 = ["MSG11", "MSG12"]
        msg2 = ["MSG21", "MSG22"]
        messages = [(msg1, msg2)[idx].pop(0) for idx in order] + ["NOMATCH"]
        channel = channel_factory(messages)
        for _ in range(4):
            assert not block_read.done()
            with channel.assert_consume():
                assert block_read.try_consume(channel)
        assert block_read.done()
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
            assert not block_write.done()
            with channel.assert_consume():
                assert block_write.try_consume(channel)
        assert block_write.done()
        assert channel.msg_buffer_names() == expected_sends
        channel.reset()
        assert block_write.done()
        assert not block_write.can_consume(channel)

    @pytest.mark.parametrize("order", itertools.permutations((0, 0, 1, 1), 4))
    @pytest.mark.parametrize("reset_idx", range(1, 5))
    def test_block_reset(self, block_read, order, reset_idx):
        msg1 = ["MSG11", "MSG12"]
        msg2 = ["MSG21", "MSG22"]
        messages = [(msg1, msg2)[idx].pop(0) for idx in order] + ["NOMATCH"]
        channel = channel_factory(messages)
        _test_block_reset_deterministic_end(block_read, channel, reset_idx)


class TestPlainBlock:
    @pytest.fixture()
    def block(self):
        return PlainBlock([
            ClientLine(1, "C: MSG1", "MSG1"),
            ClientLine(2, "C: MSG2", "MSG2"),
            ServerLine(3, "S: SMSG", "SMSG"),
            ClientLine(4, "C: MSG3", "MSG3"),
        ], 1)

    @pytest.fixture()
    def channel(self):
        return channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])

    def test_block(self, block, channel):
        for _ in range(2):
            assert not block.done()
            assert not channel.msg_buffer
            with channel.assert_consume():
                assert block.try_consume(channel)
        assert not block.done()
        assert channel.msg_buffer_names() == ["SMSG"]
        with channel.assert_consume():
            assert block.try_consume(channel)
        assert block.done()
        assert not block.can_consume(channel)
        assert channel.msg_buffer_names() == ["SMSG"]

    @pytest.mark.parametrize("reset_idx", range(1, 3))
    def test_block_reset(self, block, channel, reset_idx):
        _test_block_reset_deterministic_end(block, channel, reset_idx)


class _TestRepeatBlock:
    must_run_once = None
    block_cls = None

    @pytest.fixture()
    def block_1(self):
        return self.block_cls(BlockList([
            PlainBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
            ], 2)
        ], 2), 1)

    @pytest.fixture()
    def channel_1(self):
        return channel_factory(["MSG1", "NOMATCH"])

    @pytest.fixture()
    def block_2(self):
        return self.block_cls(BlockList([
            PlainBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2")
            ], 2)
        ], 2), 1)

    @pytest.fixture()
    def channel_2(self):
        return channel_factory(["MSG1", "MSG2", "NOMATCH"])

    @pytest.fixture()
    def block_optional_end(self):
        return self.block_cls(BlockList([
            PlainBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2"),
            ], 2),
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(5, "C: MSG3", "MSG3")], 5)
            ], 5), 4),
        ], 2), 1)

    @pytest.fixture()
    def channel_optional_end(self):
        return channel_factory(["MSG1", "MSG2",  "MSG3", "NOMATCH"])

    @pytest.fixture()
    def block_all_optional(self):
        return self.block_cls(BlockList([
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(3, "C: MSG1", "MSG1")], 3)
            ], 3), 2),
            OptionalBlock(BlockList([
                PlainBlock([ClientLine(6, "C: MSG2", "MSG2")], 6)
            ], 6), 5),
        ], 2), 1)

    @pytest.fixture()
    def channel_all_optional(self):
        return channel_factory(["MSG1", "MSG2", "NOMATCH"])

    def _test_loop_run(self, block, channel, skippable=None):
        assert len(channel.messages) >= 1
        if skippable is None:
            skippable = set()
        for run in range(2):
            if run == 0 and self.must_run_once and 0 not in skippable:
                assert not block.can_be_skipped()
            else:
                assert block.can_be_skipped()
            with channel.assert_consume():
                assert block.try_consume(channel)
            for step in range(1, len(channel.messages) - 1):
                if step in skippable:
                    assert block.can_be_skipped()
                else:
                    assert not block.can_be_skipped()
                with channel.assert_consume():
                    assert block.try_consume(channel)
            assert block.can_be_skipped()
            channel.reset()

    def _test_loop_run_with_skip(self, block, channel, skip_idx):
        assert len(channel.messages) >= 1
        for run in range(2):
            with channel.assert_consume():
                assert block.try_consume(channel)
            for step in range(1, skip_idx):
                with channel.assert_consume():
                    assert block.try_consume(channel)
            assert block.can_be_skipped()
            channel.reset()

    def test_loop_1(self, block_1, channel_1):
        self._test_loop_run(block_1, channel_1)

    def test_loop_2(self, block_2, channel_2):
        self._test_loop_run(block_2, channel_2)

    def test_loop_optional_end(self, block_optional_end, channel_optional_end):
        self._test_loop_run(block_optional_end, channel_optional_end, {2})

    @pytest.mark.parametrize("skip_idx", (2,))
    def test_loop_skip_optional_end(self, block_optional_end,
                                    channel_optional_end, skip_idx):
        self._test_loop_run_with_skip(block_optional_end, channel_optional_end,
                                      skip_idx)

    def test_loop_all_optional(self, block_all_optional, channel_all_optional):
        self._test_loop_run(block_all_optional, channel_all_optional, {0, 1})

    @pytest.mark.parametrize("skip_idx", (0, 1))
    def test_loop_skip_all_optional(self, block_all_optional,
                                    channel_all_optional, skip_idx):
        self._test_loop_run_with_skip(block_all_optional, channel_all_optional,
                                      skip_idx)

    def test_cant_skip_too_soon(self, block_optional_end, channel_optional_end):
        if self.must_run_once:
            assert not block_optional_end.can_be_skipped()
        else:
            assert block_optional_end.can_be_skipped()
        assert block_optional_end.try_consume(channel_optional_end)
        assert not block_optional_end.can_be_skipped()
        channel_optional_end.reset()
        assert not block_optional_end.can_be_skipped()
        assert not block_optional_end.try_consume(channel_optional_end)
        assert not block_optional_end.can_be_skipped()


class TestRepeat0Block(_TestRepeatBlock):
    must_run_once = False
    block_cls = Repeat0Block


class TestRepeat1Block(_TestRepeatBlock):
    must_run_once = True
    block_cls = Repeat1Block
