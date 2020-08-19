"""
Define TLS suites
"""

import unittest
import tests.tls.casigned as casigned

loader = unittest.TestLoader()

protocol4x0 = unittest.TestSuite()
protocol4x0.addTests(loader.loadTestsFromModule(casigned))
