import lark
import pytest
from typing import (
    Iterator,
    Optional,
    Tuple,
)

from .. import parsing


def assert_plain_block(block, lines=None):
    assert isinstance(block, parsing.PlainBlock)
    if lines is not None:
        assert [line.canonical() for line in block.lines] == lines


def assert_plain_block_block_list(block_list, lines=None):
    assert isinstance(block_list, parsing.BlockList)
    assert len(block_list.blocks) == 1
    assert_plain_block(block_list.blocks[0], lines=lines)


def whitespace_generator(n: int,
                         optional_with_nl: Optional[set],
                         optional_without_nl: Optional[set]) -> \
        Iterator[Tuple[str]]:
    if optional_with_nl is None:
        optional_with_nl = set()
    if optional_without_nl is None:
        optional_without_nl = set()
    if optional_without_nl and (min(optional_without_nl) < 0
                                or max(optional_without_nl) > n - 1):
        raise ValueError("optional_without_nl out of bounds")
    if optional_with_nl and (min(optional_with_nl) < 0
                             or max(optional_with_nl) > n - 1):
        raise ValueError("optional_with_nl out of bounds")
    if optional_without_nl & optional_with_nl:
        raise ValueError("line in optional_without_nl and optional_with_nl")

    opt_replacements = ("", " ", "\t", "\n", "\t\n\r\n  \t \n ")
    no_nl_replacements = ("", " ", "\t", "\t ")
    req_replacements = (" \n", "\n ", "\t\n\r\n   \n")

    base = ["\n"] * n
    for i in optional_with_nl | optional_without_nl:
        base[i] = ""
    yield tuple(base)
    for i in range(n):
        if i in optional_without_nl:
            replacements = no_nl_replacements
        elif i in optional_with_nl:
            replacements = opt_replacements
        else:
            replacements = req_replacements
        for replacement in replacements:
            yield *base[:i], replacement, *base[(i + 1):]
    for i in range(n):
        if i in optional_without_nl:
            base[i] = no_nl_replacements[-1]
        elif i in optional_with_nl:
            base[i] = opt_replacements[-1]
        else:
            base[i] = req_replacements[-1]
    yield tuple(base)


VALID_BANGS = ("!: AUTO HELLO", "!: BOLT 4.0",
               "!: ALLOW RESTART", "!: ALLOW CONCURRENT",
               "!: HANDSHAKE 00\tfF 0204")
INVALID_BANGS = ("!: NOPE", "!: HANDSHAKE \x00\x00\x02\x04",
                 "!: BOLT", "!: BOLT a.b")
BANG_DEFAULTS = {"auto": set(), "bolt_version": None,
                 "restarting": False, "concurrent": False,
                 "handshake": None}
BANG_EFFECTS = (
    ("auto", {"HELLO"}),
    ("bolt_version", (4, 0)),
    ("restarting", True),
    ("concurrent", True),
    ("handshake", b"\x00\xff\x02\x04")
)


@pytest.mark.parametrize("whitespaces", whitespace_generator(1, {0}, set()))
def test_empty_script_is_invalid(whitespaces):
    with pytest.raises(lark.ParseError):
        parsing.parse(whitespaces[0])


@pytest.mark.parametrize("whitespaces", whitespace_generator(2, {0, 1}, set()))
@pytest.mark.parametrize("bang", VALID_BANGS)
def test_only_bang_script_is_invalid(whitespaces, bang):
    with pytest.raises(lark.ParseError):
        parsing.parse(whitespaces[0] + bang + whitespaces[1])


good_fields = (
    "null",
    "1",
    "1.9",
    "-4",
    '"a"',
    '"None"',
    '["a", 2]',
    '{"a": 1, "b": "c"}',
)
bad_fields = (
    "None",
    "1,9",
    "'a'",
    "['a', 2]",
    "{\"a\": 1, 'b': \"c\"}",
)


@pytest.mark.parametrize(("fields", "fail"), (
    *(([field], False) for field in good_fields),
    *(([field], True) for field in bad_fields),
    *(([gf, bf], True) for gf in good_fields for bf in bad_fields),
    *(([bf, gf], True) for gf in good_fields for bf in bad_fields),
))
@pytest.mark.parametrize("line_type", ("C:", "S:"))
@pytest.mark.parametrize("extra_ws", whitespace_generator(3, None, {0, 1, 2}))
def test_message_fields(line_type, fields, fail, extra_ws):
    if len(fields) == 1:
        script = "%s" + line_type + " MSG %s" + fields[0] + "%s"
    elif len(fields) == 2:
        script = line_type + " MSG %s" + " %s".join(fields) + "%s"
    else:
        raise ValueError()
    script = script % extra_ws
    if fail:
        with pytest.raises(parsing.LineError):
            parsing.parse(script)
    else:
        script = parsing.parse(script)
        assert_plain_block_block_list(script.block_list, [
            "%s MSG %s" % (line_type, " ".join(fields))
        ])


@pytest.mark.parametrize("extra_ws", whitespace_generator(5, {0, 4}, {1, 3}))
def test_simple_dialogue(extra_ws):
    script = "%sC:%sMSG1%sS:%sMSG2%s" % extra_ws
    script = parsing.parse(script)
    assert_plain_block_block_list(script.block_list, ["C: MSG1", "S: MSG2"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(6, {0, 5}, set()))
def test_simple_alternative_block(extra_ws):
    script = "%s{{%sC:MSG1%s----%sC:MSG2%s}}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.AlternativeBlock)
    assert len(block.block_lists) == 2
    assert_plain_block_block_list(block.block_lists[0], ["C: MSG1"])
    assert_plain_block_block_list(block.block_lists[1], ["C: MSG2"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(6, {0, 5}, set()))
def test_simple_parallel_block(extra_ws):
    script = "%s{{%sC:MSG1%s++++%sC:MSG2%s}}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.ParallelBlock)
    assert len(block.block_lists) == 2
    assert_plain_block_block_list(block.block_lists[0], ["C: MSG1"])
    assert_plain_block_block_list(block.block_lists[1], ["C: MSG2"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, set()))
def test_simple_optional_block(extra_ws):
    script = "%s{?%sC:MSG1%s?}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.OptionalBlock)
    assert_plain_block_block_list(block.block_list, ["C: MSG1"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, set()))
def test_simple_0_loop(extra_ws):
    script = "%s{*%sC:MSG1%s*}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.Repeat0Block)
    assert_plain_block_block_list(block.block_list, ["C: MSG1"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, set()))
def test_simple_1_loop(extra_ws):
    script = "%s{+%sC:MSG1%s+}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.Repeat1Block)
    assert_plain_block_block_list(block.block_list, ["C: MSG1"])


@pytest.mark.parametrize("bang", zip(VALID_BANGS, BANG_EFFECTS))
@pytest.mark.parametrize("extra_ws", whitespace_generator(3, {0, 2}, set()))
def test_simple_bang_line(bang, extra_ws):
    bang, bang_effect = bang
    expected_context = BANG_DEFAULTS.copy()
    expected_context.update(dict((bang_effect,)))

    script = ("%s" + bang + "%sC: MSG%s") % extra_ws
    script = parsing.parse(script)
    assert_plain_block_block_list(script.block_list, ["C: MSG"])
    assert script.context.__dict__ == expected_context


@pytest.mark.parametrize("bang", INVALID_BANGS)
def test_invalid_bangs_raise(bang):
    script = (bang + "\n\nC: MSG")
    with pytest.raises(lark.GrammarError):
        parsing.parse(script)


@pytest.mark.parametrize("bang", VALID_BANGS)
def test_bang_must_come_first(bang):
    with pytest.raises(lark.LarkError):
        parsing.parse("C: MSG\n" + bang)


@pytest.mark.parametrize("outer", (
    (parsing.AlternativeBlock, "{{", "----", "}}"),
    (parsing.ParallelBlock, "{{", "++++", "}}"),
    (parsing.OptionalBlock, "{?", "?}"),
    (parsing.Repeat0Block, "{*", "*}"),
    (parsing.Repeat1Block, "{+", "+}")
))
@pytest.mark.parametrize("inner", (
    (parsing.AlternativeBlock, "{{", "----", "}}"),
    (parsing.ParallelBlock, "{{", "++++", "}}"),
    (parsing.OptionalBlock, "{?", "?}"),
    (parsing.Repeat0Block, "{*", "*}"),
    (parsing.Repeat1Block, "{+", "+}")
))
def test_nested_blocks(outer, inner):
    script = "S: MSG1\n" + outer[1] + "\n" + inner[1] + "\n"
    if len(inner) == 4:
        script += "C: MSG2.1\n" + inner[2] + "\nC: MSG2.2\n"
    else:
        script += "C: MSG2\n"
    script += inner[-1] + "\n"
    if len(outer) == 4:
        script += outer[2] + "\nC: MSG3\n" + outer[3] + "\nC: MSG4"
    else:
        script += outer[2] + "\nC: MSG3"

    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 3
    assert_plain_block(script.block_list.blocks[0], ["S: MSG1"])
    assert_plain_block(script.block_list.blocks[2], [
        "C: MSG4" if len(outer) == 4 else "C: MSG3"
    ])
    outer_block = script.block_list.blocks[1]
    assert isinstance(outer_block, outer[0])
    if len(outer) == 4:
        assert len(outer_block.block_lists) == 2
        assert_plain_block_block_list(outer_block.block_lists[1], ["C: MSG3"])
        assert len(outer_block.block_lists[0].blocks) == 1
        inner_block = outer_block.block_lists[0].blocks[0]
    else:
        assert len(outer_block.block_list.blocks) == 1
        inner_block = outer_block.block_list.blocks[0]
    assert isinstance(inner_block, inner[0])
    if len(inner) == 4:
        assert len(inner_block.block_lists) == 2
        assert_plain_block_block_list(inner_block.block_lists[0], ["C: MSG2.1"])
        assert_plain_block_block_list(inner_block.block_lists[1], ["C: MSG2.2"])
    else:
        assert_plain_block_block_list(inner_block.block_list, ["C: MSG2"])


@pytest.mark.parametrize("block_parts", (
    (parsing.OptionalBlock, "{?", "?}"),
    (parsing.Repeat0Block, "{*", "*}"),
    (parsing.Repeat1Block, "{+", "+}")
))
@pytest.mark.parametrize(("end_line", "fail"), (
    ("C: YIP", False),
    ("S: NOPE", True),
))
def test_line_after_nondeterministic_end_block(block_parts, end_line, fail):
    script = """C: MSG1
    %s
        C: MSG2
    %s
    %s""" % (*block_parts[1:], end_line)

    if fail:
        with pytest.raises(lark.LarkError,
                           match=r".*ambiguity.*" + end_line + ".*"):
            parsing.parse(script)
    else:
        script = parsing.parse(script)
        assert len(script.block_list.blocks) == 3
        assert_plain_block(script.block_list.blocks[0], ["C: MSG1"])
        assert_plain_block(script.block_list.blocks[2], [end_line])
        assert isinstance(script.block_list.blocks[1], block_parts[0])


@pytest.mark.parametrize("block_parts", (
    (parsing.AlternativeBlock, "{{", "----", "}}"),
    (parsing.ParallelBlock, "{{", "++++", "}}")
))
@pytest.mark.parametrize("end_line", ("C: YIP", "S: FINE_TOO"))
def test_line_after_deterministic_end_block(block_parts, end_line):
    script = """C: MSG1
    %s
        C: MSG2
    %s
        C: MSG3
    %s
    %s""" % (*block_parts[1:], end_line)

    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 3
    assert_plain_block(script.block_list.blocks[0], ["C: MSG1"])
    assert_plain_block(script.block_list.blocks[2], [end_line])
    block = script.block_list.blocks[1]
    assert isinstance(block, block_parts[0])
    assert len(block.block_lists) == 2
    assert_plain_block_block_list(block.block_lists[0], ["C: MSG2"])
    assert_plain_block_block_list(block.block_lists[1], ["C: MSG3"])


@pytest.mark.parametrize("block_parts", (
    (parsing.AlternativeBlock, "{{", "----", "}}"),
    (parsing.ParallelBlock, "{{", "++++", "}}")
))
def test_long_multi_blocks(block_parts):
    script = block_parts[1] + "\n"
    for i in range(98):
        script += ("    C: MSG%i\n" % (i + 1)) + block_parts[2] + "\n"
    script += "    C: MSG99\n" + block_parts[3]

    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, block_parts[0])
    assert len(block.block_lists) == 99
    for i in range(99):
        assert_plain_block_block_list(block.block_lists[i],
                                      ["C: MSG%i" % (i + 1)])


@pytest.mark.parametrize(("block_parts", "swap"), (
    ((parsing.AlternativeBlock, "{{", "----", "}}"), False),
    ((parsing.ParallelBlock, "{{", "++++", "}}"), True),
    ((parsing.AlternativeBlock, "{{", "----", "}}"), False),
    ((parsing.ParallelBlock, "{{", "++++", "}}"), True),
    ((parsing.OptionalBlock, "{?", "?}"), False),
    ((parsing.Repeat0Block, "{*", "*}"), False),
    ((parsing.Repeat1Block, "{+", "+}"), False),
))
def test_block_cant_start_with_server_line(block_parts, swap):
    script = block_parts[1] + "\n"
    if len(block_parts) == 4:
        if swap:
            script += "    C: CMSG\n" + block_parts[2] + "\n    S: SMSG\n"
        else:
            script += "    S: SMSG\n" + block_parts[2] + "\n    C: CMSG\n"
    else:
        script += "S: SMSG\n"
    script += block_parts[-1]

    with pytest.raises(lark.LarkError, match=r".*ambiguity.*S: SMSG.*"):
        parsing.parse(script)


def test_script_can_start_with_server_line():
    script = parsing.parse("S: MSG\nC: MSG2")
    assert_plain_block_block_list(script.block_list, ["S: MSG", "C: MSG2"])


@pytest.mark.parametrize("extra_ws",
                         whitespace_generator(15, {0, 14}, {1, 4, 6, 8, 12},))
def test_implicit_line_type(extra_ws):
    script = (
        "{}S:{}SMSG1{}"
        "SMSG2{}"
        "S:{}SMSG3{}"
        "C:{}MSG1{}"
        "C:{}MSG2{}"
        "MSG3{}"
        "MSG4{}"
        "C:{}MSG5{}"
        "MSG6{}".format(*extra_ws)
    )
    script = parsing.parse(script)
    assert_plain_block_block_list(script.block_list, [
        "S: SMSG1",
        "S: SMSG2",
        "S: SMSG3",
        "C: MSG1",
        "C: MSG2",
        "C: MSG3",
        "C: MSG4",
        "C: MSG5",
        "C: MSG6",
    ])
