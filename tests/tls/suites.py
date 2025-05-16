"""Define TLS suite."""

import sys
import unittest

from tests.testenv import get_test_result_class
from tests.tls import (
    test_client_certificate,
    test_secure_scheme,
    test_self_signed_scheme,
    test_tls_versions,
    test_unsecure_scheme,
)

loader = unittest.TestLoader()

tls_suite = unittest.TestSuite()
tls_suite.addTests(loader.loadTestsFromModule(test_secure_scheme))
tls_suite.addTests(loader.loadTestsFromModule(test_self_signed_scheme))
tls_suite.addTests(loader.loadTestsFromModule(test_tls_versions))
tls_suite.addTests(loader.loadTestsFromModule(test_unsecure_scheme))
tls_suite.addTests(loader.loadTestsFromModule(test_client_certificate))

if __name__ == "__main__":
    suite_name = "TLS tests"
    runner = unittest.TextTestRunner(
        resultclass=get_test_result_class(suite_name),
        verbosity=100, stream=sys.stdout,
    )
    result = runner.run(tls_suite)
    if result.errors or result.failures:
        sys.exit(-1)
