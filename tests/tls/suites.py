"""
Define TLS suite
"""

import unittest
import sys
import tests.tls.securescheme as securescheme
import tests.tls.selfsignedscheme as selfsignedscheme
import tests.tls.unsecurescheme as unsecurescheme
import tests.tls.tlsversions as tlsversions
from tests.testenv import get_test_result_class, begin_test_suite, end_test_suite

loader = unittest.TestLoader()

tls_suite = unittest.TestSuite()
tls_suite.addTests(loader.loadTestsFromModule(securescheme))
tls_suite.addTests(loader.loadTestsFromModule(selfsignedscheme))
tls_suite.addTests(loader.loadTestsFromModule(unsecurescheme))
tls_suite.addTests(loader.loadTestsFromModule(tlsversions))

if __name__ == "__main__":
    suiteName = "TLS tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(
            resultclass=get_test_result_class(), verbosity=100)
    result = runner.run(tls_suite)
    end_test_suite(suiteName)
    if result.errors or result.failures:
        sys.exit(-1)
