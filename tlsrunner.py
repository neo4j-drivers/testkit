import os, unittest
from tests.tls.suites import tls_suite
from testenv import get_test_result_class, begin_test_suite, end_test_suite, in_teamcity
#import suites

if __name__ == "__main__":
    suiteName = "TLS tests"
    begin_test_suite(suiteName)
    runner = unittest.TextTestRunner(resultclass=get_test_result_class(), verbosity=100)
    result = runner.run(tls_suite)
    #if result.errors or result.failures:
    #    failed = True
    end_test_suite(suiteName)

