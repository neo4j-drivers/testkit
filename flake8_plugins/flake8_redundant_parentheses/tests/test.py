import ast
import io
import tokenize
from typing import Set

from parentheses_checker import Plugin
import pytest


def _results(s: str) -> Set[str]:
    def read_lines():
        return s.splitlines(keepends=True)

    tree = ast.parse(s)
    file_tokens = tokenize.tokenize(io.BytesIO(s.encode("utf-8")).readline)
    plugin = Plugin(tree, read_lines, file_tokens)
    return {f"{line}:{col + 1} {msg}" for line, col, msg, _ in plugin.run()}


def _ws_generator():
    return (ws for ws in ("", " ", "\t", "   "))


def test_multi_line_condition():
    s = """if (foo
            == bar):
        ...
    """
    assert not _results(s)


# GOOD (use parentheses for tuple literal)
def test_tuple_literal_1():
    s = """a = ("a",)
    """
    assert not _results(s)


# GOOD (use parentheses for tuple literal)
def test_tuple_literal_2():
    s = """a = ("a", "b")
    """
    assert not _results(s)


# GOOD (use parentheses for tuple literal)
def test_tuple_literal_3():
    s = """a = (\\
    "a", "b")
    """
    assert not _results(s)


# BAD (one pair of parentheses for tuple literal is enough)
def test_multi_parens_tuple_literal_1():
    s = """a = (("a", "b"))
    """
    assert len(_results(s)) == 2


# BAD (one pair of parentheses for tuple literal is enough)
def test_multi_parens_tuple_literal_2():
    s = """a = ((
        "a", "b"
    ))
    """
    assert len(_results(s)) == 2


# GOOD (parentheses for tuple literal are optional)
def test_tuple_literal_optional_1():
    s = """a = "a",
    """
    assert not _results(s)


# GOOD (parentheses for tuple literal are optional)
def test_tuple_literal_optional_2():
    s = """a = "a", "b"
    """
    assert not _results(s)


# GOOD (parentheses for tuple literal are optional)
def test_tuple_literal_unpacking():
    s = """a, b = "a", "b"
    """
    assert not _results(s)


# BAD (redundant parentheses for unpacking)
def test_ugly_multiline_unpacking():
    s = """(
a, b\
) = 1, 2"""
    assert len(_results(s)) == 1


# GOOD (parentheses for tuple literal are optional)
def test_tuple_literal_unpacking_in_if():
    s = """if (foo):
        a, b = "a", "b"
    """
    assert len(_results(s)) == 0


# GOOD (parentheses are redundant, but can help readability)
def test_bin_op_example_1():
    s = """a = (1 + 2) % 3
    """
    assert not _results(s)


# GOOD (parentheses are necessary)
def test_bin_op_example_2():
    s = """a = 1 + (2 % 3)
    """
    assert not _results(s)


# GOOD (parentheses might be redundant, but can help readability)
def test_bin_op_example_3():
    s = """a = 1 + (2 and 3)
    """
    assert not _results(s)


# GOOD (parentheses might be redundant, but can help readability)
def test_bin_op_example_4():
    s = """a = (1 + 2) and 3
    """
    assert not _results(s)


# GOOD (parentheses might be redundant, but can help readability)
def test_bin_op_example_5():
    s = """a = 1 and (2 + 3)
    """
    assert not _results(s)


# GOOD (parentheses might be redundant, but can help readability)
def test_bin_op_example_6():
    s = """a = (1 and 2) + 3
    """
    assert not _results(s)


# GOOD (parentheses are redundant, but can help readability)
def test_bin_op_example_7():
    s = """a = foo or (bar and baz)
    """
    assert not _results(s)


# GOOD with ugly spaces (parentheses are redundant, but can help readability)
def test_bin_op_example_8():
    s = """a = (   1 /   2)   /3
    """
    assert not _results(s)


# BAD (parentheses are redundant and can't help readability)
def test_bin_op_example_unnecessary_parens():
    s = """a = (foo or bar and baz)
    """
    assert len(_results(s)) == 1


# GOOD (parentheses are redundant and can help readability)
def test_multi_line_bin_op_example_unnecessary_parens():
    # OK, please don't judge me for this incredibly ugly code...
    # I need to make a point here.
    s = """a = foo + (\\
bar * baz
)
    """
    assert not _results(s)


# BAD (don't use parentheses for unpacking)
@pytest.mark.parametrize("ws1", _ws_generator())
@pytest.mark.parametrize("ws2", _ws_generator())
@pytest.mark.parametrize("ws3", _ws_generator())
@pytest.mark.parametrize("ws4", _ws_generator())
@pytest.mark.parametrize("ws5", _ws_generator())
def test_unpacking(ws1, ws2, ws3, ws4, ws5):
    s = f"""({ws1}a{ws2},{ws3}){ws4}={ws5}["a"]
    """
    assert len(_results(s)) == 1


# BAD (don't use parentheses for unpacking, even with leading white space)
def test_unpacking_with_white_space():
    s = """(\ta,)=["a"]
    """
    assert len(_results(s)) == 1


# BAD (don't use parentheses for one-line expressions)
def test_one_line_condition():
    s = """if (foo == bar):
        a + b
    """
    assert len(_results(s)) == 1


# BAD (don't use parentheses for one-line expressions)
def test_one_line_expression_1():
    s = """a = (foo == bar)
    """
    assert len(_results(s)) == 1


# BAD (don't use parentheses for one-line expressions)
def test_one_line_expression_2():
    s = """a = (foo.bar())
    """
    assert len(_results(s)) == 1


# GOOD (function call)
def test_function_call():
    s = """foo("a")
    """
    assert not _results(s)


# BAD (function call with extra parentheses)
def test_function_call_redundant_parens_1():
    s = """foo(("a"))
    """
    assert len(_results(s)) == 2


# BAD (function call with extra parentheses around expression)
def test_function_call_redundant_parens_2():
    s = """foo((1 + 2))
    """
    assert len(_results(s)) == 2


# BAD
def test_function_call_redundant_parens_3():
    s = """foo((1 + 2), 3)
    """
    assert len(_results(s)) == 1


# GOOD
def test_function_call_redundant_parens_for_readability():
    s = """foo((1 + 2) + 3, 4)
    """
    assert not _results(s)


# GOOD
def test_multi_line_list():
    s = """[
        1
        + 2,
        3
    ]
    """
    assert not _results(s)


# BAD
def test_multi_line_list_unnecessary_parens_1():
    s = """[
        (1
         + 2),
        3
    ]
    """
    assert len(_results(s)) == 1


# BAD
def test_multi_line_list_unnecessary_parens_2():
    s = """[
        (
            1
            + 2
        ),
        3
    ]
    """
    assert len(_results(s)) == 1


# BAD
def test_function_call_unnecessary_multi_line_parens():
    s = """foo(
        (1 + 2) + 3,
        (4
         + 5)
    )
    """
    assert len(_results(s)) == 1


# GOOD (function call with tuple literal)
def test_function_call_with_tuple():
    s = """foo(("a",))
    """
    assert not _results(s)


# GOOD (method call)
def test_method_call():
    s = """foo.bar("a")
    """
    assert not _results(s)


# GOOD (use parentheses for line continuation)
def test_multi_line_parens_1():
    s = """a = ("abc"
         "def")
    """
    assert not _results(s)


# GOOD (use parentheses for line continuation)
def test_multi_line_parens_2():
    s = """a = (
        "abc"
        "def"
    )
    """
    assert not _results(s)


# BAD (parentheses are redundant and can't help readability)
def test_unnecessary_parens():
    s = """a = ("a")
    """
    assert len(_results(s)) == 1


# BAD (one pair of parenthesis is enough)
def test_bin_op_example_double_parens_1():
    s = """a = 1 * ((2 + 3))
    """
    assert len(_results(s)) == 1


# BAD (one pair of parenthesis is enough)
def test_bin_op_example_double_parens_2():
    s = """a = ((1 * 2)) + 3
    """
    assert len(_results(s)) == 1


# BAD (one pair of parenthesis is enough)
def test_bin_op_example_double_parens_3():
    s = """a = 1 + ((2 * 3))
    """
    assert len(_results(s)) == 1


# BAD (one pair of parenthesis is enough)
def test_bin_op_example_double_parens_4():
    s = """a = ((1 + 2)) * 3
    """
    assert len(_results(s)) == 1


# BAD (redundant parenthesis around 1)
def test_redundant_parens_around_tuple():
    s = """a = ((1),)
    """
    assert len(_results(s)) == 1


# GOOD (generator comprehension)
def test_generator_comprehension():
    s = """a = (foo for foo in bar)
    """
    assert not _results(s)


# BAD
def test_unary_op_example_1():
    s = """a = not (b)
    """
    assert len(_results(s)) == 1


# BAD
def test_unary_op_example_2():
    s = """a = (not b)
    """
    assert len(_results(s)) == 1


# GOOD (parentheses might be redundant, but can help readability)
def test_mixed_op_example_1():
    s = """a = not (1 + 2)
    """
    assert not _results(s)


# GOOD (parentheses might be redundant, but can help readability)
def test_mixed_op_example_2():
    s = """a = (not 1) + 2
    """
    assert not _results(s)


# GOOD (parentheses might be redundant, but can help readability)
def test_mixed_op_example_3():
    s = """a = not 1 + 2
    """
    assert not _results(s)


# BAD (two redundant parentheses)
def test_wildly_nested_parens():
    s = """a = 1 + (2 + (3) + (4))
    """
    assert len(_results(s)) == 2


BIN_OPS = ("**", "*", "@", "/", "//", "%", "+", "-", "<<",
           ">>", "&", "^", "|", "in", "is", "is not", "<",
           "<=", ">", ">=", "!=", "==", "and", "or")
UNARY_OPS = ("not", "+", "-", "~", "await")


def _id(s):
    return s


def _make_multi_line(s):
    assert s.startswith("foo = ")
    return f"foo = (\n    {s[6:]}\n)"


def _make_multi_line_extra_parens_1(s):
    assert s.startswith("foo = ")
    return f"foo = ((\n    {s[6:]}\n))"


def _make_multi_line_extra_parens_2(s):
    assert s.startswith("foo = ")
    return f"foo = (\n    ({s[6:]})\n)"


MULTI_LINE_ALTERATION = (
    # (function, makes it bad)
    (_id, 0),
    (_make_multi_line, 0),
    (_make_multi_line_extra_parens_1, 2),
    (_make_multi_line_extra_parens_2, 1),
)


@pytest.mark.parametrize("op", UNARY_OPS)
@pytest.mark.parametrize("alteration", MULTI_LINE_ALTERATION)
def test_superfluous_parentheses_after_mono_op(op, alteration):
    alteration_func, introduced_flakes = alteration
    s = f"foo = {op} (bar)"
    s = alteration_func(s)
    assert len(_results(s)) == 1 + introduced_flakes


@pytest.mark.parametrize("op", UNARY_OPS)
@pytest.mark.parametrize("op2", BIN_OPS + UNARY_OPS)
@pytest.mark.parametrize("alteration", MULTI_LINE_ALTERATION)
def test_superfluous_but_helping_parentheses_after_mono_op(
    op, op2, alteration
):
    alteration_func, introduced_flakes = alteration
    if op2 in UNARY_OPS:
        expression = f"{op2} bar"
    else:
        expression = f"foo {op2} bar"
    s = f"foo = {op} ({expression})"
    s = alteration_func(s)
    assert len(_results(s)) == introduced_flakes


@pytest.mark.parametrize("op", BIN_OPS)
@pytest.mark.parametrize("alteration", MULTI_LINE_ALTERATION)
def test_superfluous_parentheses_around_bin_op(op, alteration):
    alteration_func, introduced_flakes = alteration
    s = f"foo = (foo {op} bar)"
    s = alteration_func(s)
    assert len(_results(s)) == 1 + introduced_flakes


@pytest.mark.parametrize("op1", BIN_OPS)
@pytest.mark.parametrize("op2", BIN_OPS)
@pytest.mark.parametrize("parens_first", (True, False))
@pytest.mark.parametrize("alteration", MULTI_LINE_ALTERATION)
def test_superfluous_but_helping_parentheses_around_bin_op(
    op1, op2, parens_first, alteration
):
    alteration_func, introduced_flakes = alteration
    parent_expr = f"(foo {op1} bar)"
    if parens_first:
        s = f"foo = {parent_expr} {op2} baz"
    else:
        s = f"foo = baz {op2} {parent_expr}"
    s = alteration_func(s)
    assert len(_results(s)) == introduced_flakes


@pytest.mark.parametrize("op1", BIN_OPS)
@pytest.mark.parametrize("op2", BIN_OPS)
@pytest.mark.parametrize("parens_first", (True, False))
@pytest.mark.parametrize("alteration", MULTI_LINE_ALTERATION)
def test_double_superfluous_but_helping_parentheses_around_bin_op(
    op1, op2, parens_first, alteration
):
    alteration_func, introduced_flakes = alteration
    parent_expr = f"((foo {op1} bar))"
    if parens_first:
        s = f"foo = {parent_expr} {op2} baz"
    else:
        s = f"foo = baz {op2} {parent_expr}"
    s = alteration_func(s)
    assert len(_results(s)) == 1 + introduced_flakes
