"""
Define TLS suites
"""

import unittest
import tests.tls.casigned as casigned
import tests.tls.selfsigned as selfsigned

loader = unittest.TestLoader()

protocol4x0 = unittest.TestSuite()
protocol4x0.addTests(loader.loadTestsFromModule(casigned))
protocol4x0.addTests(loader.loadTestsFromModule(selfsigned))
