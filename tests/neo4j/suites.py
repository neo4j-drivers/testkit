"""
Defines suites of test to run in different setups
"""

import unittest
import tests.neo4j.datatypes as datatypes

loader = unittest.TestLoader()

"""
Suite for Neo4j 4.0 single instance community edition
"""
single_community_neo4j4x0 = unittest.TestSuite()
single_community_neo4j4x0.addTests(loader.loadTestsFromModule(datatypes))


"""
Suite for Neo4j 4.1 single instance enterprise edition
"""
single_enterprise_neo4j4x1 = unittest.TestSuite()
single_enterprise_neo4j4x1.addTests(loader.loadTestsFromModule(datatypes))

