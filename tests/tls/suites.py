"""
Define TLS suite
"""

import sys
import unittest

from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)
from tests.tls import test_secure_scheme
from tests.tls import test_self_signed_scheme
from tests.tls import test_tls_versions
from tests.tls import test_unsecure_scheme


loader = unittest.TestLoader()

tls_suite = unittest.TestSuite()
tls_suite.addTests(loader.loadTestsFromModule(test_secure_scheme))
tls_suite.addTests(loader.loadTestsFromModule(test_self_signed_scheme))
tls_suite.addTests(loader.loadTestsFromModule(test_tls_versions))
tls_suite.addTests(loader.loadTestsFromModule(test_unsecure_scheme))

if __name__ == "__main__":
    suiteName = "TLS tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(),
                                     verbosity=100)
    result = runner.run(tls_suite)
    end_test_suite(suiteName)
    if result.errors or result.failures:
        sys.exit(-1)

