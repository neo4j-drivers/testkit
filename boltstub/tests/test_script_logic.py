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
    Line,
    OptionalBlock,
    ParallelBlock,
    ClientBlock,
    ServerBlock,
    Repeat0Block,
    Repeat1Block,
)


class MockChannel:
    def __init__(self, messages: Iterable[TranslatedStructure]):
        self.messages = tuple(messages)
        self.index = 0
        self.raw_buffer = bytearray()
        self.msg_buffer = []

    def __repr__(self):
        return "MockChannel[{}][{}]".format(
            ", ".join(map(lambda m: m.name, self.messages[:self.index])),
            ", ".join(map(lambda m: m.name, self.messages[self.index:]))

        )

    def send_raw(self, b):
        self.raw_buffer.extend(b)

    def send_struct(self, struct):
        self.msg_buffer.append(struct)

    def send_server_line(self, server_line):
        msg = TranslatedStructure(*Line.parse_line(server_line))
        self.msg_buffer.append(msg)

    def msg_buffer_names(self):
        return [msg.name for msg in self.msg_buffer]

    def consume(self):
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


def _assert_accepted_messages(block, messages):
    assert list(map(lambda line: line.content, block.accepted_messages())) \
           == messages


class TestAlternativeBlock:
    @pytest.fixture()
    def block_read(self):
        return AlternativeBlock(
            [
                BlockList([ClientBlock([
                    ClientLine(2, "C: MSG1", "MSG1")
                ], 2)], 2),
                BlockList([ClientBlock([
                    ClientLine(3, "C: MSG2", "MSG2")
                ], 3)], 3),
            ], 1
        )

    @pytest.fixture()
    def block_write(self):
        return AlternativeBlock(
            [
                BlockList([
                    ClientBlock([
                        ClientLine(2, "C: MSG1", "MSG1"),
                    ], 2),
                    ServerBlock([
                        ServerLine(3, "S: SMSG1", "SMSG1")
                    ], 3),
                ], 2),
                BlockList([
                    ClientBlock([
                        ClientLine(4, "C: MSG2", "MSG2"),
                    ], 4),
                    ServerBlock([
                        ServerLine(5, "S: SMSG2", "SMSG2")
                    ], 5),
                ], 5),
            ], 1
        )

    @pytest.fixture()
    def block_with_non_det_end(self):
        return AlternativeBlock([
            BlockList([
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2),
                ServerBlock([ServerLine(3, "S: SMSG1", "SMSG1")], 3),
                OptionalBlock(BlockList([
                    ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                    ServerBlock([ServerLine(6, "S: SMSG2", "SMSG2")], 6)
                ], 5), 4),
            ], 2),
            BlockList([
                ClientBlock([ClientLine(8, "C: MSG3", "MSG3")], 8),
                ServerBlock([ServerLine(9, "S: SMSG3", "SMSG3")], 9),
            ], 7)
        ], 1)

    @pytest.fixture()
    def block_with_non_det_block(self):
        return AlternativeBlock([
            BlockList([
                OptionalBlock(BlockList([
                    ClientBlock([ClientLine(3, "C: MSG1", "MSG1")], 3),
                    ServerBlock([ServerLine(4, "S: SMSG1", "SMSG1")], 4)
                ], 3), 2),
            ], 2),
            BlockList([
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                ServerBlock([ServerLine(7, "S: SMSG2", "SMSG2")], 7),
            ], 5)
        ], 1)

    @pytest.mark.parametrize("branch", range(2))
    def test_block_read(self, block_read, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        branch_channel = (msg1_channel, msg2_channel)[branch]
        assert not block_read.done()
        assert block_read.can_consume(msg1_channel)
        assert block_read.can_consume(msg2_channel)
        _assert_accepted_messages(block_read, ["MSG1", "MSG2"])
        with branch_channel.assert_consume():
            assert block_read.try_consume(branch_channel)
        assert block_read.done()
        assert not block_read.can_consume(msg1_channel)
        assert not block_read.can_consume(msg2_channel)
        branch_channel.reset()
        assert not block_read.can_consume(branch_channel)
        _assert_accepted_messages(block_read, [])

    @pytest.mark.parametrize("branch", range(2))
    def test_block_write(self, block_write, branch):
        msg1_channel = channel_factory(["MSG1", "NOMATCH"])
        msg2_channel = channel_factory(["MSG2", "NOMATCH"])
        expected_message = ("SMSG1", "SMSG2")[branch]
        branch_channel = (msg1_channel, msg2_channel)[branch]
        _assert_accepted_messages(block_write, ["MSG1", "MSG2"])
        with branch_channel.assert_consume():
            assert block_write.try_consume(branch_channel)
        _assert_accepted_messages(block_write, [])
        assert branch_channel.msg_buffer_names() == [expected_message]
        assert block_write.done()
        assert not block_write.can_consume(msg1_channel)
        assert not block_write.can_consume(msg2_channel)
        branch_channel.reset()
        assert not block_write.can_consume(branch_channel)
        _assert_accepted_messages(block_write, [])

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

    @pytest.mark.parametrize("messages", (("1", "2"), ("1",), ("3",)))
    def test_non_det_ending_alternative(self, block_with_non_det_end, messages):
        channel = channel_factory(["MSG%s" % m for m in messages] + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_end.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_end.can_consume(channel)
        if block_with_non_det_end.has_deterministic_end():
            assert block_with_non_det_end.done()
        else:
            assert block_with_non_det_end.can_be_skipped()

    @pytest.mark.parametrize("messages", (("1",), ("2",), ()))
    def test_non_det_alternative(self, block_with_non_det_block, messages):
        channel = channel_factory(["MSG%s" % m for m in messages] + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_block.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_block.can_consume(channel)
        if block_with_non_det_block.has_deterministic_end():
            assert block_with_non_det_block.done()
        else:
            assert block_with_non_det_block.can_be_skipped()


class TestBlockList:
    @pytest.fixture()
    def block_det(self):
        return BlockList([
            ClientBlock([
                ClientLine(1, "C: MSG1", "MSG1"),
                ClientLine(2, "C: MSG2", "MSG2")
            ], 1),
            ServerBlock([
                ServerLine(3, "S: SMSG1", "SMSG1"),
                ServerLine(4, "S: SMSG2", "SMSG2")
            ], 3),
            ClientBlock([ClientLine(5, "C: MSG3", "MSG3")], 5),
        ], 1)

    @pytest.fixture()
    def block_non_det(self):
        return BlockList([
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2)
            ], 2), 1),
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5)
            ], 5), 4),
            ClientBlock([ClientLine(7, "C: MSG3", "MSG3")], 7),
        ], 1)

    @pytest.fixture()
    def block_only_non_det(self):
        return BlockList([
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2)
            ], 2), 1),
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5)
            ], 5), 4),
            OptionalBlock(BlockList([
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
            assert not block_det.done()
            assert not block_det.can_be_skipped()
            _assert_accepted_messages(block_det, [messages[i]])
            with channel.assert_consume():
                assert block_det.try_consume(channel)
            assert not block_det.can_consume(channel_factory(messages[i:]))
            if i < 1:
                assert not channel.msg_buffer
            else:
                assert channel.msg_buffer_names() == ["SMSG1", "SMSG2"]
        assert block_det.done()
        assert block_det.can_be_skipped()
        _assert_accepted_messages(block_det, [])

    @pytest.mark.parametrize("skip1", (True, False))
    @pytest.mark.parametrize("skip2", (True, False))
    def test_consecutive_non_deterministic(self, skip1, skip2, block_non_det):
        all_messages = ["MSG1", "MSG2", "MSG3", "NOMATCH"]
        messages = TestBlockList._skip_messages(all_messages, skip1, skip2)
        channel = channel_factory(messages)
        assert block_non_det.has_deterministic_end()
        accepted = all_messages[:-1]
        for i in range(len(messages) - 1):
            assert not block_non_det.done()
            assert not block_non_det.can_be_skipped()
            _assert_accepted_messages(block_non_det, accepted)
            accepted = all_messages[(all_messages.index(messages[i]) + 1):-1]
            assert not block_non_det.done()
            with channel.assert_consume():
                assert block_non_det.try_consume(channel)
            assert not block_non_det.can_consume(channel_factory(messages[i:]))
        assert block_non_det.can_be_skipped()
        assert block_non_det.done()
        _assert_accepted_messages(block_non_det, [])

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
            _assert_accepted_messages(block_only_non_det, accepted)
            accepted = all_messages[(all_messages.index(messages[i]) + 1):-1]
            with pytest.raises(RuntimeError):
                assert not block_only_non_det.done()
            assert block_only_non_det.can_be_skipped()
            with channel.assert_consume():
                assert block_only_non_det.try_consume(channel)
            assert not block_only_non_det.can_consume(channel_factory(
                messages[i:]
            ))
        assert block_only_non_det.can_be_skipped()
        _assert_accepted_messages(block_only_non_det, accepted)
        if not skip3:
            # consumed last message => block should be done
            assert block_only_non_det.has_deterministic_end()
            assert block_only_non_det.done()
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
        block = OptionalBlock(BlockList([ClientBlock([
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
        block = OptionalBlock(BlockList([ClientBlock([
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        _test_block_reset_nondeterministic_end(block, channel, reset_idx,
                                               skippable={0})

    def test_block_can_skip_optional_end(self):
        block = OptionalBlock(BlockList([
            ClientBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2")
            ], 2),
            OptionalBlock(BlockList([
                ClientBlock([
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
        block = OptionalBlock(BlockList([ClientBlock([
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        assert not block.has_deterministic_end()
        assert block.try_consume(channel)
        assert block.has_deterministic_end()
        assert not block.done()
        assert block.try_consume(channel)
        assert block.has_deterministic_end()
        assert block.done()

    def test_accepted_messages(self):
        block = OptionalBlock(BlockList([ClientBlock([
            ClientLine(2, "C: MSG1", "MSG1"), ClientLine(3, "C: MSG2", "MSG2")
        ], 2)], 2), 1)
        channel = channel_factory(["MSG1", "MSG2", "NOMATCH"])
        _assert_accepted_messages(block, ["MSG1"])
        assert block.try_consume(channel)
        _assert_accepted_messages(block, ["MSG2"])
        assert block.try_consume(channel)
        _assert_accepted_messages(block, [])


class TestParallelBlock:
    @pytest.fixture()
    def block_read(self):
        return ParallelBlock(
            [
                BlockList([ClientBlock([
                    ClientLine(2, "C: MSG11", "MSG11"),
                    ClientLine(2, "C: MSG12", "MSG12")
                ], 2)], 2),
                BlockList([ClientBlock([
                    ClientLine(3, "C: MSG21", "MSG21"),
                    ClientLine(3, "C: MSG21", "MSG22")
                ], 3)], 3),
            ], 1
        )

    @pytest.fixture()
    def block_write(self):
        return ParallelBlock(
            [
                BlockList([
                    ClientBlock([
                        ClientLine(2, "C: MSG1", "MSG1"),
                    ], 2),
                    ServerBlock([
                        ServerLine(3, "S: SMSG1", "SMSG1")
                    ], 3),
                ], 2),
                BlockList([
                    ClientBlock([
                        ClientLine(4, "C: MSG2", "MSG2"),
                    ], 4),
                    ServerBlock([
                        ServerLine(5, "S: SMSG2", "SMSG2")
                    ], 5),
                ], 5),
            ], 1
        )

    @pytest.fixture()
    def block_with_non_det_end(self):
        return ParallelBlock([
            BlockList([
                ClientBlock([ClientLine(2, "C: MSG1", "MSG1")], 2),
                ServerBlock([ServerLine(3, "S: SMSG1", "SMSG1")], 3),
                OptionalBlock(BlockList([
                    ClientBlock([ClientLine(5, "C: MSG2", "MSG2")], 5),
                    ServerBlock([ServerLine(6, "S: SMSG2", "SMSG2")], 6)
                ], 5), 4),
            ], 2),
            BlockList([
                ClientBlock([ClientLine(8, "C: MSG3", "MSG3")], 8),
                ServerBlock([ServerLine(9, "S: SMSG3", "SMSG3")], 9),
            ], 7)
        ], 1)

    @pytest.fixture()
    def block_with_non_det_block(self):
        return ParallelBlock([
            BlockList([
                OptionalBlock(BlockList([
                    ClientBlock([ClientLine(3, "C: MSG1", "MSG1")], 3),
                    ServerBlock([ServerLine(4, "S: SMSG1", "SMSG1")], 4)
                ], 3), 2),
            ], 2),
            BlockList([
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
            accepted = msg1[idxs[0]:(idxs[0] + 1)] + msg2[idxs[1]:(idxs[1] + 1)]
            _assert_accepted_messages(block_read, accepted)
            idxs[order[i]] += 1
            assert not block_read.done()
            with channel.assert_consume():
                assert block_read.try_consume(channel)
        _assert_accepted_messages(block_read, [])
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

    @pytest.mark.parametrize("messages", (
        ("1", "2", "3"), ("1", "3"),
    ))
    def test_non_det_ending_alternative(self, block_with_non_det_end, messages):
        channel = channel_factory(["MSG%s" % m for m in messages] + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_end.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_end.can_consume(channel)
        if block_with_non_det_end.has_deterministic_end():
            assert block_with_non_det_end.done()
        else:
            assert block_with_non_det_end.can_be_skipped()

    @pytest.mark.parametrize("messages", (
        ("1", "2"), ("2",),
    ))
    def test_non_det_alternative(self, block_with_non_det_block, messages):
        channel = channel_factory(["MSG%s" % m for m in messages] + ["NOMATCH"])
        for i in range(len(messages)):
            assert block_with_non_det_block.try_consume(channel)
            assert channel.msg_buffer_names()[-1] == "SMSG%s" % messages[i]
        assert len(channel.msg_buffer) == len(messages)
        assert not block_with_non_det_block.can_consume(channel)
        if block_with_non_det_block.has_deterministic_end():
            assert block_with_non_det_block.done()
        else:
            assert block_with_non_det_block.can_be_skipped()


class TestClientBlock:
    @pytest.fixture()
    def single_block(self):
        return ClientBlock([
            ClientLine(1, "C: MSG1", "MSG1"),
        ], 1)

    @pytest.fixture()
    def single_channel(self):
        return channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])

    @pytest.fixture()
    def multi_block(self):
        return ClientBlock([
            ClientLine(1, "C: MSG1", "MSG1"),
            ClientLine(2, "C: MSG2", "MSG2"),
            ClientLine(3, "C: MSG3", "MSG3"),
        ], 1)

    @pytest.fixture()
    def multi_channel(self):
        return channel_factory(["MSG1", "MSG2", "MSG3", "NOMATCH"])

    def test_single_block(self, single_block, single_channel):
        assert single_block.can_consume(single_channel)
        assert not single_block.done()
        assert not single_channel.msg_buffer
        assert single_block.can_consume(single_channel)
        _assert_accepted_messages(single_block, ["MSG1"])
        single_block.init(single_channel)
        assert not single_block.done()
        assert not single_channel.msg_buffer
        _assert_accepted_messages(single_block, ["MSG1"])
        assert single_block.can_consume(single_channel)
        with single_channel.assert_consume():
            assert single_block.try_consume(single_channel)
        _assert_accepted_messages(single_block, [])
        assert single_block.done()
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)

    def test_multi_block(self, multi_block, multi_channel):
        assert multi_block.can_consume(multi_channel)
        assert not multi_block.done()
        assert not multi_channel.msg_buffer
        _assert_accepted_messages(multi_block, ["MSG1"])
        multi_block.init(multi_channel)
        assert not multi_block.done()
        assert not multi_channel.msg_buffer
        for i in range(3):
            _assert_accepted_messages(multi_block,
                                      ["MSG1", "MSG2", "MSG3"][i:(i + 1)])
            assert multi_block.can_consume(multi_channel)
            with multi_channel.assert_consume():
                assert multi_block.try_consume(multi_channel)
        _assert_accepted_messages(multi_block, [])
        assert multi_block.done()
        assert not multi_block.can_consume(multi_channel)
        assert not multi_block.try_consume(multi_channel)


class TestServerBlock:
    @pytest.fixture()
    def single_block(self):
        return ServerBlock([
            ServerLine(1, "S: SMSG1", "SMSG1"),
        ], 1)

    @pytest.fixture()
    def single_channel(self):
        return channel_factory(["SMSG1", "NOMATCH"])

    @pytest.fixture()
    def multi_block(self):
        return ServerBlock([
            ServerLine(1, "S: SMSG1", "SMSG1"),
            ServerLine(2, "S: SMSG2", "SMSG2"),
            ServerLine(3, "S: SMSG3", "SMSG3"),
        ], 1)

    @pytest.fixture()
    def multi_channel(self):
        return channel_factory(["NOMATCH"])

    def test_single_block(self, single_block, single_channel):
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)
        assert not single_block.done()
        assert not single_channel.msg_buffer
        _assert_accepted_messages(single_block, [])
        single_block.init(single_channel)
        _assert_accepted_messages(single_block, [])
        assert single_block.done()
        assert single_channel.msg_buffer_names() == ["SMSG1"]
        assert not single_block.can_consume(single_channel)
        assert not single_block.try_consume(single_channel)

    def test_multi_block(self, multi_block, multi_channel):
        assert not multi_block.can_consume(multi_channel)
        assert not multi_block.try_consume(multi_channel)
        assert not multi_block.done()
        assert not multi_channel.msg_buffer
        _assert_accepted_messages(multi_block, [])
        multi_block.init(multi_channel)
        _assert_accepted_messages(multi_block, [])
        assert multi_block.done()
        assert multi_channel.msg_buffer_names() == ["SMSG1", "SMSG2", "SMSG3"]
        assert not multi_block.can_consume(multi_channel)
        assert not multi_block.try_consume(multi_channel)


class _TestRepeatBlock:
    must_run_once = None
    block_cls = None

    @pytest.fixture()
    def block_1(self):
        return self.block_cls(BlockList([
            ClientBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
            ], 2)
        ], 2), 1)

    @pytest.fixture()
    def channel_1(self):
        return channel_factory(["MSG1", "NOMATCH"])

    @pytest.fixture()
    def block_2(self):
        return self.block_cls(BlockList([
            ClientBlock([
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
            ClientBlock([
                ClientLine(2, "C: MSG1", "MSG1"),
                ClientLine(3, "C: MSG2", "MSG2"),
            ], 2),
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(5, "C: MSG3", "MSG3")], 5)
            ], 5), 4),
        ], 2), 1)

    @pytest.fixture()
    def channel_optional_end(self):
        return channel_factory(["MSG1", "MSG2",  "MSG3", "NOMATCH"])

    @pytest.fixture()
    def block_all_optional(self):
        return self.block_cls(BlockList([
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(3, "C: MSG1", "MSG1")], 3)
            ], 3), 2),
            OptionalBlock(BlockList([
                ClientBlock([ClientLine(6, "C: MSG2", "MSG2")], 6)
            ], 6), 5),
        ], 2), 1)

    @pytest.fixture()
    def channel_all_optional(self):
        return channel_factory(["MSG1", "MSG2", "NOMATCH"])

    def _accepted_messages(self, channel: MockChannel, skippable):
        messages = [m.name for m in channel.messages if m.name != "NOMATCH"]
        assert messages
        accepted = [channel.index] if channel.index < len(messages) else []
        for i in range(channel.index, len(messages) * 2):
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
        if skippable is None:
            skippable = set()
        for run in range(2):
            _assert_accepted_messages(
                block, self._accepted_messages(channel, skippable)
            )
            if run == 0 and self.must_run_once and 0 not in skippable:
                assert not block.can_be_skipped()
            else:
                assert block.can_be_skipped()
            with channel.assert_consume():
                assert block.try_consume(channel)
            for step in range(1, len(channel.messages) - 1):
                _assert_accepted_messages(
                    block, self._accepted_messages(channel, skippable)
                )
                if step in skippable:
                    assert block.can_be_skipped()
                else:
                    assert not block.can_be_skipped()
                with channel.assert_consume():
                    assert block.try_consume(channel)
            assert block.can_be_skipped()
            _assert_accepted_messages(
                block, self._accepted_messages(channel, skippable)
            )
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
