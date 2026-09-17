"""Defines stub suites."""

import os
import sys
import unittest

from tests.testenv import (
    get_test_result_class,
    shard_suite,
)

loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()

stub_suite.addTest(loader.discover(
    "tests.stub",
    top_level_dir=os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", ".."
    ))
))

suite_name = "Stub tests"

shard = os.environ.get("TEST_SHARD")
if shard:
    stub_suite = shard_suite(stub_suite, shard)
    suite_name += " (shard %s)" % (shard,)

if __name__ == "__main__":
    runner = unittest.TextTestRunner(
        resultclass=get_test_result_class(suite_name),
        verbosity=100, stream=sys.stdout,
    )
    result = runner.run(stub_suite)
    if result.errors or result.failures:
        sys.exit(-1)
