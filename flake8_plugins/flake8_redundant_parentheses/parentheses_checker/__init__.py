import ast
import importlib.metadata
from typing import (
    Any,
    Generator,
    List,
    Tuple,
    Type,
)


class Checker:
    def __init__(self, vals, tree):
        self.problems: List[Tuple[int, int, str]] = []
        self.vals = vals
        self.tree = tree
        assert isinstance(self.tree, ast.Module)

    def check(self) -> None:
        msg = "PAR001: Too many parentheses"
        # exceptions made for parentheses that are not strictly necessary
        # but help readability
        exceptions = []
        bin_exc = (ast.BinOp, ast.BoolOp, ast.UnaryOp, ast.Compare, ast.Await)
        for node_ in ast.walk(self.tree):
            if isinstance(node_, bin_exc):
                for node_op in ast.iter_child_nodes(node_):
                    if not isinstance(node_op, bin_exc):
                        break
                    for val in self.vals:
                        parens_range = range(val[0], val[1])
                        if (
                            node_.col_offset in parens_range
                            and node_.end_col_offset - 1 in parens_range
                        ):
                            break
                        if (
                            node_op.col_offset in parens_range
                            and node_op.end_col_offset - 1 in parens_range
                        ):
                            exceptions.append(val[:2])
                            break

            if isinstance(node_, ast.Assign):
                for targ in node_.targets:
                    if isinstance(targ, ast.Tuple):
                        for elts in targ.elts:
                            if elts.col_offset > 0:
                                self.problems.append((
                                    node_.lineno, node_.col_offset,
                                    "PAR002: Dont use parentheses for "
                                    "unpacking"
                                ))
                            break

            for node_tup in ast.iter_child_nodes(node_):
                if (isinstance(node_tup, ast.Constant)
                   and not isinstance(node_, ast.Call)):
                    if (
                        node_.end_col_offset - node_tup.end_col_offset == 1
                        and isinstance(node_tup.value, int)
                    ):
                        self.problems.append((
                            node_.lineno, node_.col_offset, msg))
                if isinstance(node_tup, ast.Tuple):
                    if node_tup.end_col_offset - node_.end_col_offset == 0:
                        exceptions.append([node_tup.col_offset,
                                           node_tup.end_col_offset - 1])
                        break

        for node_ in self.tree.body:
            if exceptions:
                for exception in exceptions:
                    for val in self.vals:
                        if val[2] is True and val[:2] != exception:
                            self.problems.append(
                                (node_.lineno, node_.col_offset, msg))
                        elif val[:2] == exception:
                            self.vals.remove(val)
                        continue

            elif not self.vals:
                break

            for exception in self.vals:
                if exception[2] is False:
                    continue
                else:
                    self.problems.append(
                        (node_.lineno, node_.col_offset, msg))
                    continue


class Plugin:
    name = __name__
    version = importlib.metadata.version("flake8_redundant_parentheses")

    def __init__(self, tree: ast.AST, read_lines, file_tokens):
        self._tree = tree
        self.vals = []
        self.dump_tree = ast.dump(tree)
        self.parens_coords = check(list(file_tokens))
        self._lines_list = "".join(read_lines())
        for coords in self.parens_coords:
            self.vals.append(
                check_trees(self._lines_list, self.dump_tree, coords)
            )

    def run(self) -> Generator[Tuple[int, int, str, Type[Any]], None, None]:
        checker = Checker(self.vals, self._tree)
        checker.check()
        for line, col, msg in checker.problems:
            yield line, col, msg, type(self)


def check(token):
    open_list = ["[", "{", "("]
    close_list = ["]", "}", ")"]
    opening_stack = []
    parentheses_pairs = []
    for i in token:
        if i.type == 54:
            if i.string in open_list:
                opening_stack.append([i.start, i.string])
            if i.string in close_list:
                opening = opening_stack.pop()
                assert (open_list.index(opening[1])
                        == close_list.index(i.string))
                parentheses_pairs.append([opening[0], i.start])

    return parentheses_pairs


def check_trees(source_code, start_tree, parens_coords):
    """Check if parentheses are redundant.

    Replace a pair of parentheses with a blank string and check if the
    resulting AST is still the same.
    """
    open_, close = parens_coords
    lines = source_code.split("\n")
    lines[open_[0] - 1] = (lines[open_[0] - 1][:open_[1]]
                           + " " + lines[open_[0] - 1][open_[1] + 1:])
    lines[close[0] - 1] = (lines[close[0] - 1][:close[1]]
                           + " " + lines[close[0] - 1][close[1] + 1:])
    code_without_parens = "\n".join(lines)
    try:
        tree = ast.parse(code_without_parens)
    except (ValueError, SyntaxError):
        return [open_[1], close[1], False]
    if ast.dump(tree) == start_tree:
        return [open_[1], close[1], True]
    else:
        return [open_[1], close[1], False]
