"""Defines suites of test to run in different setups."""

import os
import re
import sys
import unittest

from tests.neo4j.shared import env_neo4j_version
from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)

#######################
# Suite for Neo4j 4.2 #
#######################
loader = unittest.TestLoader()

suite_4x2 = unittest.TestSuite()

suite_4x2.addTest(loader.discover(
    "tests.neo4j",
    top_level_dir=os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", ".."
    ))
))

#######################
# Suite for Neo4j 4.3 #
#######################
suite_4x3 = suite_4x2

#######################
# Suite for Neo4j 4.4 #
#######################
suite_4x4 = suite_4x3

#######################
# Suite for Neo4j 5.0 #
#######################
suite_5x0 = suite_4x3

#######################
# Suite for Neo4j 5.5 #
#######################
suite_5x5 = suite_5x0

#######################
# Suite for Neo4j 5.7 #
#######################
suite_5x7 = suite_5x5


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Missing suite name parameter")
        sys.exit(-10)
    name = sys.argv[1]
    match = re.match(r"(\d+)\.dev", name)
    if match:
        version = (int(match.group(1)), float("inf"))
    else:
        try:
            version = tuple(int(i) for i in name.split("."))
        except ValueError:
            print(f"Invalid suite name: {name}. "
                  "Should be X.Y for X and Y integer.")
            sys.exit(-2)
    if version >= (5, 7):
        suite = suite_5x7
    elif version >= (5, 5):
        suite = suite_5x5
    elif version >= (5, 0):
        suite = suite_5x0
    elif version >= (4, 4):
        suite = suite_4x4
    elif version >= (4, 3):
        suite = suite_4x3
    elif version >= (4, 2):
        suite = suite_4x2
    else:
        print("Unknown suite name: " + name)
        sys.exit(-1)

    import os
    os.environ[env_neo4j_version] = os.environ.get(env_neo4j_version, name)

    suite_name = "Integration tests " + name
    begin_test_suite(suite_name)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(),
                                     verbosity=100)
    result = runner.run(suite)
    end_test_suite(suite_name)
    if result.errors or result.failures:
        sys.exit(-1)
