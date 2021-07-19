"""
Defines stub suites
"""

import os
import sys
import unittest

from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)


loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()

stub_suite.addTest(loader.discover(
    "tests.stub",
    top_level_dir=os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", ".."
    ))
))

if __name__ == "__main__":
    suiteName = "Stub tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(),
                                     verbosity=100)
    result = runner.run(stub_suite)
    end_test_suite(suiteName)
    if result.errors or result.failures:
        sys.exit(-1)
