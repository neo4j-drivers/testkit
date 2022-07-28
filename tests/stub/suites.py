"""Defines stub suites."""

import os
import sys
import unittest

import xmlrunner

from tests.testenv import (
    begin_test_suite,
    end_test_suite,
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
    suite_name = "Stub tests"
    begin_test_suite(suite_name)
    runner = xmlrunner.XMLTestRunner(
        verbosity=100,
        output="./artifacts/reports/",
        outsuffix="stub"
    )
    result = runner.run(stub_suite)
    end_test_suite(suite_name)
    if result.errors or result.failures:
        sys.exit(-1)
