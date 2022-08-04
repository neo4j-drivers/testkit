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
        for node_ in ast.walk(self.tree):
            if isinstance(node_, (ast.BinOp, ast.BoolOp, ast.UnaryOp,
                                  ast.Compare, ast.Await)):
                for node_op in ast.iter_child_nodes(node_):
                    if isinstance(node_op, (ast.BinOp, ast.BoolOp, ast.UnaryOp,
                                            ast.Compare, ast.Await)):
                        exceptions.append(
                            [node_op.col_offset - 1, node_op.end_col_offset]
                        )

            if isinstance(node_, ast.Assign):
                for targ in node_.targets:
                    if isinstance(targ, ast.Tuple):
                        for elts in targ.elts:
                            if elts.col_offset != 0 and not self.problems:
                                self.problems.append((
                                    node_.lineno, node_.col_offset,
                                    "PAR002: Dont use parentheses for "
                                    "unpacking"
                                ))
                            break

            for node_tup in ast.iter_child_nodes(node_):
                if isinstance(node_tup, ast.Tuple):
                    if node_tup.end_col_offset - node_.end_col_offset == 0:
                        exceptions.append([node_tup.col_offset,
                                           node_tup.end_col_offset - 1])
                        break

        for node_ in self.tree.body:
            if exceptions:
                for exception in exceptions:
                    for val in self.vals:
                        if val is not False and val != exception:
                            self.problems.append(
                                (node_.lineno, node_.col_offset, msg))
                        continue

            elif not self.vals:
                break

            elif not self.problems:
                for exception in self.vals:
                    if exception is False:
                        continue
                    else:
                        self.problems.append(
                            (node_.lineno, node_.col_offset, msg))
                        break


class Plugin:
    name = __name__
    version = importlib.metadata.version("flake8_redundant_parentheses")

    def __init__(self, tree: ast.AST, read_lines, file_tokens):
        self._tree = tree
        self.vals = []
        self.dump_tree = ast.dump(tree)
        self.dict = check(list(file_tokens))
        self._lines_list = "".join(read_lines())
        for i in self.dict.values():
            self.vals.append(check_trees(self._lines_list, self.dump_tree, i))

    def run(self) -> Generator[Tuple[int, int, str, Type[Any]], None, None]:
        checker = Checker(self.vals, self._tree)
        checker.check()
        for line, col, msg in checker.problems:
            yield line, col, msg, type(self)


def check(token):
    open_cord_dict = []
    close_cord_dict = []
    result_ = {}
    open_list = ["[", "{", "("]
    close_list = ["]", "}", ")"]
    num = 0
    for i in token:
        if i.type == 54:
            if i.string in open_list:
                open_cord_dict.append([i.start, i.string])
            if i.string in close_list:
                close_cord_dict.append([i.start, i.string])

    for open_, close in zip(reversed(open_cord_dict), close_cord_dict):
        if open_list.index(open_[1]) == close_list.index(close[1]):
            result_.update({
                "{number} {type}".format(number=num, type=open_[1]):
                    [open_[0], close[0]]
            })
            num += 1
    return result_


def check_trees(list_, start_tree, parens_coords):
    """Check if parentheses are redundant.

    Replace a pair of parentheses with a blank string and check if the
    resulting AST is still the same.
    """
    list__ = list_.split("\n")
    list__[parens_coords[0][0] - 1] = (
        list__[parens_coords[0][0] - 1][:parens_coords[0][1]]
        + " " + list__[parens_coords[0][0] - 1][parens_coords[0][1] + 1:]
    )
    list__[parens_coords[1][0] - 1] = (
        list__[parens_coords[1][0] - 1][:parens_coords[1][1]]
        + " " + list__[parens_coords[1][0] - 1][parens_coords[1][1] + 1:]
    )
    list__ = "\n".join(list__)
    try:
        tree = ast.parse(list__)
    except (ValueError, SyntaxError):
        return False
    if ast.dump(tree) == start_tree:
        return [parens_coords[0][1], parens_coords[1][1]]
    else:
        return False
