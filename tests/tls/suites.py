"""
Define TLS suites
"""

import unittest
import tests.tls.securescheme as securescheme
import tests.tls.selfsignedscheme as selfsignedscheme
import tests.tls.unsecurescheme as unsecurescheme
import tests.tls.tlsversions as tlsversions

loader = unittest.TestLoader()

protocol4x0 = unittest.TestSuite()
protocol4x0.addTests(loader.loadTestsFromModule(securescheme))
protocol4x0.addTests(loader.loadTestsFromModule(selfsignedscheme))
protocol4x0.addTests(loader.loadTestsFromModule(unsecurescheme))
protocol4x0.addTests(loader.loadTestsFromModule(tlsversions))
