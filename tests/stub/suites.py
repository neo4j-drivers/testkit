"""
Defines stub suites
"""

import os
import sys
import unittest

import tests.stub.retry as retry
import tests.stub.transport.test_transport as transport
import tests.stub.tx_begin_parameters.test_tx_begin_parameters as txparameters
from tests.stub import authorization
import tests.stub.serversiderouting as serversiderouting
from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)


loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()
stub_suite.addTests(loader.loadTestsFromModule(authorization))
stub_suite.addTests(loader.loadTestsFromModule(retry))
stub_suite.addTests(loader.loadTestsFromModule(transport))
stub_suite.addTests(loader.loadTestsFromModule(txparameters))
stub_suite.addTests(loader.loadTestsFromModule(serversiderouting))

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
