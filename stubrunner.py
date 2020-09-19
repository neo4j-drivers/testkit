import os, unittest
from tests.stub.suites import stub_suite
from testenv import get_test_result_class, begin_test_suite, end_test_suite, in_teamcity
#import suites

if __name__ == "__main__":
    suiteName = "Stub tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(), verbosity=100)
    result = runner.run(stub_suite)
    #if result.errors or result.failures:
    #    failed = True
    end_test_suite(suiteName)

