"""
Defines stub suites
"""

import sys
import unittest

import tests.stub.bookmark as bookmark
import tests.stub.disconnected as disconnected
import tests.stub.iteration as iteration
import tests.stub.retry as retry
import tests.stub.routing as routing
import tests.stub.sessionparameters as sessionparameters
import tests.stub.transport as transport
import tests.stub.txparameters as txparameters
import tests.stub.versions as versions
from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    get_test_result_class,
)

loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()
stub_suite.addTests(loader.loadTestsFromModule(retry))
stub_suite.addTests(loader.loadTestsFromModule(transport))
stub_suite.addTests(loader.loadTestsFromModule(disconnected))
stub_suite.addTests(loader.loadTestsFromModule(sessionparameters))
stub_suite.addTests(loader.loadTestsFromModule(txparameters))
stub_suite.addTests(loader.loadTestsFromModule(bookmark))
stub_suite.addTests(loader.loadTestsFromModule(routing))
stub_suite.addTests(loader.loadTestsFromModule(iteration))
stub_suite.addTests(loader.loadTestsFromModule(versions))

if __name__ == "__main__":
    suiteName = "Stub tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(
            resultclass=get_test_result_class(), verbosity=100)
    result = runner.run(stub_suite)
    end_test_suite(suiteName)
    if result.errors or result.failures:
        sys.exit(-1)
