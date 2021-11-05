"""Define TLS suite."""

import sys
import unittest

from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)
from tests.tls import (
    securescheme,
    selfsignedscheme,
    tlsversions,
    unsecurescheme,
)

loader = unittest.TestLoader()

tls_suite = unittest.TestSuite()
tls_suite.addTests(loader.loadTestsFromModule(securescheme))
tls_suite.addTests(loader.loadTestsFromModule(selfsignedscheme))
tls_suite.addTests(loader.loadTestsFromModule(unsecurescheme))
tls_suite.addTests(loader.loadTestsFromModule(tlsversions))

if __name__ == "__main__":
    suite_name = "TLS tests"
    begin_test_suite(suite_name)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(),
                                     verbosity=100)
    result = runner.run(tls_suite)
    end_test_suite(suite_name)
    if result.errors or result.failures:
        sys.exit(-1)
