"""Defines suites of test to run in different setups."""
import os
import re
import sys
import unittest

from neo4j import GraphDatabase

from tests.neo4j.shared import (
    env_neo4j_bolt_port,
    env_neo4j_host,
    env_neo4j_pass,
    env_neo4j_scheme,
    env_neo4j_user,
    env_neo4j_version,
)
from tests.testenv import get_test_result_class

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

#######################
# Suite for Neo4j 5.9 #
#######################
suite_5x9 = suite_5x7


def parse_version_info(agent):
    """Parse version string into tuple of integers."""
    match = re.match(r".+/(\d+)\.(\d+).*", agent)
    if match:
        return tuple(int(x) for x in match.groups())
    else:
        raise ValueError(f"Invalid agent string: {agent}")


if __name__ == "__main__":
    scheme = os.environ.get(env_neo4j_scheme, "bolt")
    host = os.environ.get(env_neo4j_host, "localhost")
    port = os.environ.get(env_neo4j_bolt_port, 7687)
    uri = f"{scheme}://{host}:{port}"

    user = os.environ.get(env_neo4j_user, "neo4j")
    password = os.environ.get(env_neo4j_pass, "hu8ji9ko0")

    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        temp = driver.get_server_info()
        version = parse_version_info(temp.agent)

    if version >= (5, 9):
        suite = suite_5x9
    elif version >= (5, 7):
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
        print(f"Unknown suite version: {version}")
        sys.exit(-1)

    import os
    os.environ[env_neo4j_version] = f"{version[0]}.{version[1]}"

    suite_name = f"Integration tests {version[0]}.{version[1]}"
    runner = unittest.TextTestRunner(
        resultclass=get_test_result_class(suite_name),
        verbosity=100, stream=sys.stdout,
    )
    result = runner.run(suite)
    if result.errors or result.failures:
        sys.exit(-1)
