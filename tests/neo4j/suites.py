"""Defines suites of test to run in different setups."""

import os
import sys
import unittest

from tests.neo4j.shared import env_neo4j_version
from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)

#######################
# Suite for Neo4j 3.5 #
#######################
loader = unittest.TestLoader()

suite_3x5 = unittest.TestSuite()

suite_3x5.addTest(loader.discover(
    "tests.neo4j",
    top_level_dir=os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", ".."
    ))
))

#######################
# Suite for Neo4j 4.0 #
#######################
suite_4x0 = suite_3x5

#######################
# Suite for Neo4j 4.1 #
#######################
suite_4x1 = suite_4x0

#######################
# Suite for Neo4j 4.2 #
#######################
suite_4x2 = suite_4x1

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
suite_5x0 = suite_4x4


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Missing suite name parameter")
        sys.exit(-10)
    name = sys.argv[1]
    suite = None
    if name == "3.5":
        suite = suite_3x5
    if name == "4.0":
        suite = suite_4x0
    elif name == "4.1":
        suite = suite_4x1
    elif name == "4.2":
        suite = suite_4x2
    elif name == "4.3":
        suite = suite_4x3
    elif name == "4.4":
        suite = suite_4x4
    elif name == "5.0":
        suite = suite_5x0

    if not suite:
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
