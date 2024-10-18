"""Defines stub suites."""

import os
import sys
import unittest

from tests.testenv import get_test_result_class

loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()

stub_suite.addTest(loader.discover(
    "tests.stub",
    top_level_dir=os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", ".."
    ))
))

if __name__ == "__main__":
    suite_name = "Stub tests"
    runner = unittest.TextTestRunner(
        resultclass=get_test_result_class(suite_name),
        verbosity=100, stream=sys.stdout,
    )
    result = runner.run(stub_suite)
    if result.errors or result.failures:
        sys.exit(-1)
