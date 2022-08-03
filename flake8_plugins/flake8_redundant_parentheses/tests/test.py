import ast
import io
import tokenize
from typing import Set
from parentheses_checker import Plugin


def _results(s: str) -> Set[str]:
    file_tokens = tokenize.tokenize(io.BytesIO(s.encode("utf-8")).readline)
    tree = ast.parse(s)
    plugin = Plugin(tree, s, file_tokens)
    return {f'{line}:{col + 1} {msg}' for line, col, msg, _ in plugin.run()}


def test_trivial_case():
    assert _results('(a,) + (b,)') == set()


def test_parentheses_are_redundant():
    ret = _results('a = ("a")')
    assert ret == {'1:1 Too many parentheses'}


def test_func_call():
    assert _results('''open_list = {"[", "{", "("}''') == set()


def test_fun():
    assert _results('''# ( <-
a = 1 + 2''') == set()


def test_fun_2():
    assert _results('''a = b"("''') == set()


def test_func_call_with_tuple():
    assert _results('''foo(("a",))''') == set()


def test_method_call():
    assert _results('''foo.bar("a")''') == set()


def test_func_call_extra_parentheses():
    ret = _results('foo(("a"))')
    assert ret == {'1:1 Too many parentheses'}


def test_two_line_bin():
    ret = _results('''a = (
    (1 + 3)
    + 3
)''')
    assert ret == {'1:1 Too many parentheses'}


def test_parentheses_unpacking():
    ret = _results('(a,) = ["a"]')
    assert ret == {'1:1 Dont use parentheses for unpacking'}


def test_tuple_literal():
    assert _results('''a = (1 + 2) % 3''') == set()


def test_and_in_brackets():
    assert _results('''foo(a, [b])''') == set()


def test_double_star_out_brackets():
    assert _results('''a = 1 ** (2 + 3)''') == set()


def test():
    assert _results('''a = 1 + (2 + 3)''') == set()


def test_equation_with_two_brackets():
    ret = _results('''a = 1 / ((2 + 3)) * 4''')
    assert ret == {'1:1 Too many parentheses'}


def test_tuple_with_two_brackets():
    ret = _results('''a = ((1),)''')
    assert ret == {'1:1 Too many parentheses'}


def test_two_brackets_in_func():
    ret = _results('''foo(1, (2))''')
    assert ret == {'1:1 Too many parentheses'}


def test_generator_comprehension():
    assert _results('''a = (foo for foo in bar)''') == set()


def test_parentheses_in_value():
    ret = _results('''a = (foo or bar and baz)''')
    assert ret == {'1:1 Too many parentheses'}


def test_parentheses_in_one_line_exp():
    ret = _results('''a = (foo == bar)''')
    assert ret == {'1:1 Too many parentheses'}


def test_parentheses_in_one_line_expression():
    ret = _results('''a = (foo.bar())''')
    assert ret == {'1:1 Too many parentheses'}


def test_two_line_if():
    assert _results('''if (foo
        == bar): a = b''') == set()


def test_parentheses_one_line_if():
    ret = _results('''if (foo == bar): a = b''')
    assert ret == {'1:1 Too many parentheses'}


def test_one_more():
    ret = _results('''if (foo.bar()): a = b''')
    assert ret == {'1:1 Too many parentheses'}


def test_parentheses_one_line_while():
    ret = _results('''while (c in d): a = b''')
    assert ret == {'1:1 Too many parentheses'}


def test_true_while():
    assert _results('''while c in d: a = b''') == set()


def test_true_for():
    assert _results('''for x in d: a = b''') == set()
