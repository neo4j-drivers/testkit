"""Define TLS suite."""

import sys
import unittest

import xmlrunner

from tests.testenv import (
    begin_test_suite,
    end_test_suite,
)
from tests.tls import (
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

if __name__ == "__main__":
    suite_name = "TLS tests"
    begin_test_suite(suite_name)
    runner = xmlrunner.XMLTestRunner(
        verbosity=100,
        output="./artifacts/reports/",
        outsuffix="tls"
    )
    result = runner.run(tls_suite)
    end_test_suite(suite_name)
    if result.errors or result.failures:
        sys.exit(-1)
