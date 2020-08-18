"""
Define TLS suites
"""

import unittest
import tests.tls.shared as x

loader = unittest.TestLoader()

protocol4x0 = unittest.TestSuite()
protocol4x0.addTests(loader.loadTestsFromModule(x))
