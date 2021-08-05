from collections import defaultdict
import lark
import pytest
import re
from typing import (
    Iterator,
    Optional,
    Tuple,
)

from .. import (
    errors,
    parsing,
    Script,
)
from ._common import (
    ALL_REQUESTS_PER_VERSION,
    ALL_RESPONSES_PER_VERSION,
    ALL_SERVER_VERSIONS
)


@pytest.fixture()
def unverified_script(monkeypatch):
    monkeypatch.setattr(Script, "_verify_script", lambda *args, **kwargs: None)


def assert_client_block(block, lines=None):
    assert block.__class__ == parsing.ClientBlock
    if lines is not None:
        assert [line.canonical() for line in block.lines] == lines


def assert_auto_block(block, lines=None):
    assert block.__class__ == parsing.AutoBlock
    if lines is not None:
        assert [line.canonical() for line in block.lines] == lines


def assert_optional_auto_block(block, lines=None):
    assert block.__class__ == parsing.OptionalBlock
    assert len(block.block_list.blocks) == 1
    assert_auto_block(block.block_list.blocks[0], lines)


def assert_repeat0_auto_block(block, lines=None):
    assert block.__class__ == parsing.Repeat0Block
    assert len(block.block_list.blocks) == 1
    assert_auto_block(block.block_list.blocks[0], lines)


def assert_repeat1_auto_block(block, lines=None):
    assert block.__class__ == parsing.Repeat1Block
    assert len(block.block_list.blocks) == 1
    assert_auto_block(block.block_list.blocks[0], lines)


def assert_server_block(block, lines=None):
    assert block.__class__ == parsing.ServerBlock
    if lines is not None:
        assert [line.canonical() for line in block.lines] == lines


def assert_client_block_block_list(block_list, lines=None):
    assert block_list.__class__ == parsing.BlockList
    assert len(block_list.blocks) == 1
    assert_client_block(block_list.blocks[0], lines=lines)


def assert_auto_block_block_list(block_list, lines=None):
    assert block_list.__class__ == parsing.BlockList
    assert len(block_list.blocks) == 1
    assert_auto_block(block_list.blocks[0], lines=lines)


def assert_server_block_block_list(block_list, lines=None):
    assert block_list.__class__ == parsing.BlockList
    assert len(block_list.blocks) == 1
    assert_server_block(block_list.blocks[0], lines=lines)


def assert_dialogue_blocks_block_list(block_list, lines=None):
    expected_blocks = []
    for l in lines:
        if l.startswith("C:"):
            if (expected_blocks
                    and expected_blocks[-1][0] == assert_client_block):
                expected_blocks[-1][1].append(l)
            else:
                expected_blocks.append([assert_client_block, [l]])
        if l.startswith("A:"):
            if (expected_blocks
                    and expected_blocks[-1][0] == assert_auto_block):
                expected_blocks[-1][1].append(l)
            else:
                expected_blocks.append([assert_auto_block, [l]])
        if l.startswith("S:"):
            if (expected_blocks
                    and expected_blocks[-1][0] == assert_server_block):
                expected_blocks[-1][1].append(l)
            else:
                expected_blocks.append([assert_server_block, [l]])
    assert len(block_list.blocks) == len(expected_blocks)
    for i in range(len(expected_blocks)):
        expected_blocks[i][0](block_list.blocks[i], expected_blocks[i][1])


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
def test_empty_script_is_invalid(whitespaces, unverified_script):
    with pytest.raises(lark.ParseError):
        parsing.parse(whitespaces[0])


@pytest.mark.parametrize("whitespaces", whitespace_generator(2, {0, 1}, set()))
@pytest.mark.parametrize("bang", VALID_BANGS)
def test_only_bang_script_is_invalid(whitespaces, bang, unverified_script):
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
    *(([gf, gf], False) for gf in good_fields),
))
@pytest.mark.parametrize("line_type", ("C:", "S:"))
@pytest.mark.parametrize("extra_ws", whitespace_generator(3, None, {0, 1, 2}))
def test_message_fields(line_type, fields, fail, extra_ws, unverified_script):
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
        block_assert_fns = {"C:": assert_client_block_block_list,
                            "S:": assert_server_block_block_list}
        block_assert_fns[line_type](script.block_list, [
            "%s MSG %s" % (line_type, " ".join(fields))
        ])


@pytest.mark.parametrize("auto_marker", ("A", "?", "+", "*"))
@pytest.mark.parametrize("fields", (
    *([field] for field in good_fields),
    *([gf1, gf2] for gf1 in good_fields for gf2 in good_fields),
))
def test_auto_line_takes_no_fields(auto_marker, fields, unverified_script):
    script = auto_marker + ": MSG " + " ".join(fields)
    with pytest.raises(parsing.LineError):
        parsing.parse(script)


@pytest.mark.parametrize("extra_ws", whitespace_generator(7, {0, 6}, {1, 3, 5}))
def test_simple_dialogue(extra_ws, unverified_script):
    script = "%sC:%sMSG1%sS:%sMSG2%sA:%sMSG3%s" % extra_ws
    script = parsing.parse(script)
    assert_dialogue_blocks_block_list(script.block_list,
                                      ["C: MSG1", "S: MSG2", "A: MSG3"])


@pytest.mark.parametrize(("auto_marker", "wrapper_block_class"), (
    ("A", None),
    ("?", parsing.OptionalBlock),
    ("*", parsing.Repeat0Block),
    ("+", parsing.Repeat1Block),
))
@pytest.mark.parametrize("extra_ws", whitespace_generator(3, {0, 2}, {1}))
def test_auto_message_macros(extra_ws, auto_marker, wrapper_block_class,
                             unverified_script):
    script = "%%s%s:%%sMSG%%s" % auto_marker % extra_ws
    script = parsing.parse(script)
    if not wrapper_block_class:
        assert_dialogue_blocks_block_list(script.block_list, ["A: MSG"])
    else:
        block_list = script.block_list
        assert block_list.__class__ == parsing.BlockList
        assert len(block_list.blocks) == 1
        wrapper_block = block_list.blocks[0]
        assert wrapper_block.__class__ == wrapper_block_class
        assert wrapper_block.block_list.__class__ == parsing.BlockList
        assert len(wrapper_block.block_list.blocks) == 1
        inner_block = wrapper_block.block_list.blocks[0]
        assert_auto_block(inner_block, ["A: MSG"])


@pytest.mark.parametrize(("line_marker", "assert_block_type"), (
    ("C:", assert_client_block_block_list),
    ("S:", assert_server_block_block_list),
))
@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, {1}))
def test_implicit_lines(line_marker, assert_block_type, extra_ws,
                        unverified_script):
    script = "%%s%s%%sMSG1%%sMSG2%%s" % line_marker % extra_ws
    script = parsing.parse(script)
    assert_block_type(script.block_list)


@pytest.mark.parametrize("line_marker", ("A:", "?:", "*:", "+:"))
@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, {1}))
def test_no_implicit_auto_lines(line_marker, extra_ws, unverified_script):
    script = "%%s%s%%sMSG1%%sMSG2%%s" % line_marker % extra_ws
    with pytest.raises(lark.UnexpectedCharacters):
        parsing.parse(script)


@pytest.mark.parametrize("extra_ws", whitespace_generator(6, {0, 5}, set()))
def test_simple_alternative_block(extra_ws, unverified_script):
    script = "%s{{%sC:MSG1%s----%sC:MSG2%s}}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.AlternativeBlock)
    assert len(block.block_lists) == 2
    assert_client_block_block_list(block.block_lists[0], ["C: MSG1"])
    assert_client_block_block_list(block.block_lists[1], ["C: MSG2"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(6, {0, 5}, set()))
def test_simple_parallel_block(extra_ws, unverified_script):
    script = "%s{{%sC:MSG1%s++++%sC:MSG2%s}}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.ParallelBlock)
    assert len(block.block_lists) == 2
    assert_client_block_block_list(block.block_lists[0], ["C: MSG1"])
    assert_client_block_block_list(block.block_lists[1], ["C: MSG2"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, set()))
def test_simple_optional_block(extra_ws, unverified_script):
    script = "%s{?%sC:MSG1%s?}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.OptionalBlock)
    assert_client_block_block_list(block.block_list, ["C: MSG1"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, set()))
def test_simple_0_loop(extra_ws, unverified_script):
    script = "%s{*%sC:MSG1%s*}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.Repeat0Block)
    assert_client_block_block_list(block.block_list, ["C: MSG1"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(4, {0, 3}, set()))
def test_simple_1_loop(extra_ws, unverified_script):
    script = "%s{+%sC:MSG1%s+}%s" % extra_ws
    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 1
    block = script.block_list.blocks[0]
    assert isinstance(block, parsing.Repeat1Block)
    assert_client_block_block_list(block.block_list, ["C: MSG1"])


@pytest.mark.parametrize("bang", zip(VALID_BANGS, BANG_EFFECTS))
@pytest.mark.parametrize("extra_ws", whitespace_generator(3, {0, 2}, set()))
def test_simple_bang_line(bang, extra_ws, unverified_script):
    bang, bang_effect = bang
    expected_context = BANG_DEFAULTS.copy()
    expected_context.update(dict((bang_effect,)))

    script = ("%s" + bang + "%sC: MSG%s") % extra_ws
    script = parsing.parse(script)
    assert_client_block_block_list(script.block_list, ["C: MSG"])
    result = {k: v for k, v in script.context.__dict__.items()
              if k in expected_context}
    assert result == expected_context


@pytest.mark.parametrize("bang", INVALID_BANGS)
def test_invalid_bangs_raise(bang, unverified_script):
    script = (bang + "\n\nC: MSG")
    with pytest.raises(lark.GrammarError):
        parsing.parse(script)


@pytest.mark.parametrize("bang", VALID_BANGS)
def test_bang_must_come_first(bang, unverified_script):
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
def test_nested_blocks(outer, inner, unverified_script):
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
    assert_server_block(script.block_list.blocks[0], ["S: MSG1"])
    assert_client_block(script.block_list.blocks[2], [
        "C: MSG4" if len(outer) == 4 else "C: MSG3"
    ])
    outer_block = script.block_list.blocks[1]
    assert isinstance(outer_block, outer[0])
    if len(outer) == 4:
        assert len(outer_block.block_lists) == 2
        assert_client_block_block_list(outer_block.block_lists[1], ["C: MSG3"])
        assert len(outer_block.block_lists[0].blocks) == 1
        inner_block = outer_block.block_lists[0].blocks[0]
    else:
        assert len(outer_block.block_list.blocks) == 1
        inner_block = outer_block.block_list.blocks[0]
    assert isinstance(inner_block, inner[0])
    if len(inner) == 4:
        assert len(inner_block.block_lists) == 2
        assert_client_block_block_list(inner_block.block_lists[0],
                                       ["C: MSG2.1"])
        assert_client_block_block_list(inner_block.block_lists[1],
                                       ["C: MSG2.2"])
    else:
        assert_client_block_block_list(inner_block.block_list, ["C: MSG2"])


@pytest.mark.parametrize("block_parts", (
    (parsing.OptionalBlock, "{?", "?}"),
    (parsing.Repeat0Block, "{*", "*}"),
    (parsing.Repeat1Block, "{+", "+}")
))
@pytest.mark.parametrize(("end_line", "fail", "last_block_assert"), (
    ("C: YIP", False, assert_client_block),
    ("A: OR_THIS", False, assert_auto_block),
    ("?: OR_THAT", False, assert_optional_auto_block),
    ("*: OR_THE_OTHER", False, assert_repeat0_auto_block),
    ("+: OR_EVEN_THIS", False, assert_repeat1_auto_block),
    ("S: NOPE", True, None),
))
def test_line_after_nondeterministic_end_block(
        block_parts, end_line, fail, unverified_script, last_block_assert):
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
        assert_client_block(script.block_list.blocks[0], ["C: MSG1"])
        assert isinstance(script.block_list.blocks[1], block_parts[0])
        end_line = re.sub(r"^[?*+]:", "A:", end_line)
        last_block_assert(script.block_list.blocks[2], [end_line])


@pytest.mark.parametrize("block_parts", (
    (parsing.AlternativeBlock, "{{", "----", "}}"),
    (parsing.ParallelBlock, "{{", "++++", "}}")
))
@pytest.mark.parametrize("end_line", (
    "C: YIP",
    "A: OR_THIS",
    "?: OR_THAT",
    "*: OR_THE_OTHER",
    "?: OR_EVEN_THIS",
    "S: FINE_TOO",
))
def test_line_after_deterministic_end_block(block_parts, end_line,
                                            unverified_script):
    script = """C: MSG1
    %s
        C: MSG2
    %s
        C: MSG3
    %s
    %s""" % (*block_parts[1:], end_line)

    script = parsing.parse(script)
    assert len(script.block_list.blocks) == 3
    assert_client_block(script.block_list.blocks[0], ["C: MSG1"])
    if end_line.startswith("C:"):
        assert_client_block(script.block_list.blocks[2], [end_line])
    if end_line.startswith("S:"):
        assert_server_block(script.block_list.blocks[2], [end_line])
    block = script.block_list.blocks[1]
    assert isinstance(block, block_parts[0])
    assert len(block.block_lists) == 2
    assert_client_block_block_list(block.block_lists[0], ["C: MSG2"])
    assert_client_block_block_list(block.block_lists[1], ["C: MSG3"])


@pytest.mark.parametrize("block_parts", (
    (parsing.AlternativeBlock, "{{", "----", "}}"),
    (parsing.ParallelBlock, "{{", "++++", "}}")
))
def test_long_multi_blocks(block_parts, unverified_script):
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
        assert_client_block_block_list(block.block_lists[i],
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
def test_block_cant_start_with_server_line(block_parts, swap,
                                           unverified_script):
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


def test_script_can_start_with_server_line(unverified_script):
    script = parsing.parse("S: MSG\nC: MSG2")
    assert_dialogue_blocks_block_list(script.block_list, ["S: MSG", "C: MSG2"])


@pytest.mark.parametrize("extra_ws",
                         whitespace_generator(15, {0, 14}, {1, 4, 6, 8, 12},))
def test_implicit_line_type(extra_ws, unverified_script):
    raw_script = (
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
    script = parsing.parse(raw_script)
    assert_dialogue_blocks_block_list(script.block_list, [
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


def test_expects_bolt_version():
    script = """{}

    C: RUN
    S: SUCCESS""".format(
        "\n".join(bl for bl in VALID_BANGS if not bl.startswith("!: BOLT "))
    )
    with pytest.raises(lark.GrammarError) as exc:
        parsing.parse(script)
    assert isinstance(exc.value.__cause__, errors.BoltMissingVersion)


@pytest.mark.parametrize(("version", "exists"), (
    *((v, True) for v in ALL_SERVER_VERSIONS),
    ((0,), False),
    ((0, 1), False),
    ((2, 1), False),
))
def test_checks_bolt_version(version, exists):
    raw_script = """!: BOLT {}

    {}"""
    try:
        msg = "C: " \
              + next(n for v, _, n in ALL_REQUESTS_PER_VERSION if v == version)
    except StopIteration:
        msg = "C: FOOBAR"
    script = raw_script.format(".".join(map(str, version)), msg)
    if exists:
        parsing.parse(script)
    else:
        with pytest.raises(parsing.LineError) as exc:
            parsing.parse(script)
        assert exc.value.line.line_number == 1
        assert isinstance(exc.value.__cause__, errors.BoltUnknownVersion)


client_msg_names = defaultdict(set)
server_msg_names = defaultdict(set)


def set_name_dicts():
    global client_msg_names, server_msg_names
    for v, _, n in ALL_REQUESTS_PER_VERSION:
        assert n != "FOOBAR"  # will be used as example of non-existent message
        client_msg_names[v].add(n)
    for v, _, n in ALL_RESPONSES_PER_VERSION:
        assert n != "FOOBAR"  # will be used as example of non-existent message
        server_msg_names[v].add(n)


set_name_dicts()


@pytest.mark.parametrize(("version", "message", "exists"), (
    *(
          (v, "C: " + n, True)
          for v in client_msg_names for n in client_msg_names[v]
    ),
    *(
          (v, "C: " + n, False)
          for v in server_msg_names
          for n in server_msg_names[v] - client_msg_names[v]
    ),
    *((v, "C: FOOBAR", False) for v in ALL_SERVER_VERSIONS),
    *(
          (v, "S: " + n, True)
          for v in server_msg_names for n in server_msg_names[v]
    ),
    *(
          (v, "S: " + n, False)
          for v in client_msg_names
          for n in client_msg_names[v] - server_msg_names[v]
    ),
    *((v, "S: FOOBAR", False) for v in ALL_SERVER_VERSIONS),
))
def test_message_tags_are_verified(version, message, exists):
    raw_script = """!: BOLT {}

    {}"""
    script = raw_script.format(".".join(map(str, version)), message)
    if exists:
        parsing.parse(script)
    else:
        with pytest.raises(parsing.LineError) as exc:
            parsing.parse(script)
        assert exc.value.line.line_number == 3
        assert isinstance(exc.value.__cause__, errors.BoltUnknownMessage)


@pytest.mark.parametrize("version", ALL_SERVER_VERSIONS)
@pytest.mark.parametrize(("command", "exists"), (
    ("<EXIT>", True),
    ("<NOOP>", True),
    ("<RAW> FF", True),
    ("<SLEEP> 1", True),
    ("<FOOBAR> 1", False),
))
def test_checks_command_server_lines(version, command, exists):
    raw_script = """!: BOLT {}

    S: {}"""
    script = raw_script.format(".".join(map(str, version)), command)
    if exists:
        parsing.parse(script)
    else:
        with pytest.raises(parsing.LineError) as exc:
            parsing.parse(script)
        assert exc.value.line.line_number == 3


@pytest.mark.parametrize("extra_ws", whitespace_generator(3, None, {1}))
def test_comments(extra_ws):
    script = "!: BOLT 4.3\nC: HELLO%s#%sCOMMENT !%sS: SUCCESS" % extra_ws
    script = parsing.parse(script)
    assert_dialogue_blocks_block_list(script.block_list,
                                      ["C: HELLO", "S: SUCCESS"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(3, {2}, {1}))
def test_comments_at_end(extra_ws):
    script = "!: BOLT 4.3\nC: HELLO\nS: SUCCESS%s#%sCOMMENT !%s" % extra_ws
    script = parsing.parse(script)
    assert_dialogue_blocks_block_list(script.block_list,
                                      ["C: HELLO", "S: SUCCESS"])


@pytest.mark.parametrize("extra_ws", whitespace_generator(3, {0}, {1}))
def test_comment_before_bang_line(extra_ws):
    script = "%s#%sCOMMENT !%s!: BOLT 4.3\nC: HELLO" % extra_ws
    script = parsing.parse(script)
    assert_dialogue_blocks_block_list(script.block_list, ["C: HELLO"])


def test_comment_like_field(unverified_script):
    script = 'C: MSG "# NOT a comment"'
    script = parsing.parse(script)
    assert_dialogue_blocks_block_list(script.block_list,
                                      ['C: MSG "# NOT a comment"'])
