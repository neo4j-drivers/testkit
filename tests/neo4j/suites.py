"""
Defines suites of test to run in different setups
"""

import unittest
import tests.neo4j.test_datatypes as datatypes

loader = unittest.TestLoader()

"""
Suite for Neo4j single instance community edition
"""
single_community = unittest.TestSuite()
single_community.addTests(loader.loadTestsFromModule(datatypes))

