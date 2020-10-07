"""
Defines stub suites
"""

import unittest, sys
import tests.stub.retry as retry
import tests.stub.sessiondisconnected as sessiondisconnected
import tests.stub.transport as transport
import tests.stub.sessionparameters as sessionparameters
import tests.stub.txparameters as txparameters
import tests.stub.routing as routing
import tests.stub.txrun as txrun
import tests.stub.iteration as iteration
from tests.testenv import get_test_result_class, begin_test_suite, end_test_suite, in_teamcity

loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()
stub_suite.addTests(loader.loadTestsFromModule(retry))
stub_suite.addTests(loader.loadTestsFromModule(transport))
stub_suite.addTests(loader.loadTestsFromModule(sessiondisconnected))
stub_suite.addTests(loader.loadTestsFromModule(sessionparameters))
stub_suite.addTests(loader.loadTestsFromModule(txparameters))
stub_suite.addTests(loader.loadTestsFromModule(txrun))
stub_suite.addTests(loader.loadTestsFromModule(routing))
stub_suite.addTests(loader.loadTestsFromModule(iteration))

if __name__ == "__main__":
    suiteName = "Stub tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(), verbosity=100)
    result = runner.run(stub_suite)
    end_test_suite(suiteName)
    if result.errors or result.failures:
        sys.exit(-1)
