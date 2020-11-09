"""
Defines suites of test to run in different setups
"""

import unittest, sys
import tests.neo4j.datatypes as datatypes
import tests.neo4j.sessionrun as sessionrun
import tests.neo4j.authentication as authentication
from tests.testenv import get_test_result_class, begin_test_suite, end_test_suite, in_teamcity

loader = unittest.TestLoader()

"""
Suite for Neo4j 3.5
"""
suite_3x5 = unittest.TestSuite()
suite_3x5.addTests(loader.loadTestsFromModule(datatypes))
suite_3x5.addTests(loader.loadTestsFromModule(sessionrun))
suite_3x5.addTests(loader.loadTestsFromModule(authentication))

"""
Suite for Neo4j 4.0
"""
suite_4x0 = unittest.TestSuite()
suite_4x0.addTests(loader.loadTestsFromModule(datatypes))
suite_4x0.addTests(loader.loadTestsFromModule(sessionrun))
suite_4x0.addTests(loader.loadTestsFromModule(authentication))


"""
Suite for Neo4j 4.1
"""
suite_4x1 = unittest.TestSuite()
suite_4x1.addTests(loader.loadTestsFromModule(datatypes))
suite_4x1.addTests(loader.loadTestsFromModule(sessionrun))
suite_4x1.addTests(loader.loadTestsFromModule(authentication))

"""
Suite for Neo4j 4.2
"""
suite_4x2 = unittest.TestSuite()
suite_4x2.addTests(loader.loadTestsFromModule(datatypes))
suite_4x2.addTests(loader.loadTestsFromModule(sessionrun))
suite_4x2.addTests(loader.loadTestsFromModule(authentication))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Missing suite name parameter")
        sys.exit(-10)
    name = sys.argv[1]
    suite = None
    if   name == "3.5":
        suite = suite_3x5
    if   name == "4.0":
        suite = suite_4x0
    elif name == "4.1":
        suite = suite_4x1
    elif name == "4.2":
        suite = suite_4x2

    if not suite:
        print("Unknown suite name: " + name)
        sys.exit(-1)

    suite_name = "Integration tests " + name
    begin_test_suite(suite_name)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(), verbosity=100)
    result = runner.run(suite)
    end_test_suite(suite_name)
    if result.errors or result.failures:
        sys.exit(-1)
