import ast
import importlib.metadata
from typing import Any
from typing import Generator
from typing import List
from typing import Tuple
from typing import Type


class Visitor(ast.NodeVisitor):
    def __init__(self, tree, read_lines, parentheses_dict):
        self.problems: List[Tuple[int, int, str]] = []
        self.tree = tree
        self.read_lines = read_lines
        self.parentheses = parentheses_dict

    def visit_Something(self, node, msg) -> None:
        vals = []
        for i in self.parentheses.values():
            vals.append(check_trees(self.read_lines, self.tree, i))

        if True in vals:
            self.problems.append((node.lineno, node.col_offset, msg))

        self.generic_visit(node)

    def visit_Module(self, node: ast.Module) -> None:
        msg = 'Too many parentheses'
        for node_ in node.body:
            if isinstance(node_, ast.Assign):
                for targ in node_.targets:
                    if isinstance(targ, ast.Tuple):
                        self.problems.append((node_.lineno, node_.col_offset,
                                              'Dont use parentheses for unpacking'))
                        continue

            self.visit_Something(node_, msg)


class Plugin:
    name = __name__
    version = importlib.metadata.version(__name__)

    def __init__(self, tree: ast.AST, read_lines, file_tokens):
        self._tree = tree
        self.file_tokens = list(file_tokens)
        self._lines_list = "".join(read_lines)
        self.parentheses_dict = check(self.file_tokens)

    def run(self) -> Generator[Tuple[int, int, str, Type[Any]], None, None]:
        visitor = Visitor(self._tree, self._lines_list, self.parentheses_dict)
        visitor.visit(self._tree)
        for line, col, MSG in visitor.problems:
            yield line, col, MSG, type(self)


def check(token):
    open_cord_dict = {}
    close_cord_dict = {}
    result_ = {}
    open_list = ["[", "{", "("]
    close_list = ["]", "}", ")"]
    num = 0
    col = 0
    for i in token:
        if i.type == 54:
            if i.string in open_list:
                open_cord_dict.update({col: [i.start, i.string]})
            if i.string in close_list:
                close_cord_dict.update({col: [i.start, i.string]})
            col += 1

    rev_open_dict = dict(reversed(open_cord_dict.items()))
    for y in rev_open_dict.keys():
        for j in close_cord_dict.keys():
            if open_list.index(rev_open_dict.get(y)[1]) == close_list.index(close_cord_dict.get(j)[1]):
                result_.update({"{number} {type}".format
                                (number=num, type=rev_open_dict.get(y)[1]):
                                    [rev_open_dict.get(y)[0], close_cord_dict.get(j)[0]]})
                close_cord_dict.pop(j)
                num += 1
                break
    return result_


def check_trees(list_, start_tree, u):
    list__ = list_.split("\n")
    list__[u[0][0] - 1] = list__[u[0][0] - 1][:u[0][1]] + " " + list__[u[0][0] - 1][u[0][1] + 1:]
    list__[u[1][0] - 1] = list__[u[1][0] - 1][:u[1][1]] + " " + list__[u[1][0] - 1][u[1][1] + 1:]
    list__ = "\n".join(list__)
    try:
        tree = ast.parse(list__)
        if ast.dump(tree) == ast.dump(start_tree):
            return True
    except:
        return False
