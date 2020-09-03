"""
Define TLS suite
"""

import unittest
import tests.tls.securescheme as securescheme
import tests.tls.selfsignedscheme as selfsignedscheme
import tests.tls.unsecurescheme as unsecurescheme
import tests.tls.tlsversions as tlsversions

loader = unittest.TestLoader()

tls_suite = unittest.TestSuite()
tls_suite.addTests(loader.loadTestsFromModule(securescheme))
tls_suite.addTests(loader.loadTestsFromModule(selfsignedscheme))
tls_suite.addTests(loader.loadTestsFromModule(unsecurescheme))
tls_suite.addTests(loader.loadTestsFromModule(tlsversions))
