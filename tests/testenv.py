from teamcity import (
    escape,
    in_teamcity,
    TeamCityTestResult,
)


def begin_test_suite(name):
    if in_teamcity:
        print("##teamcity[testSuiteStarted name='%s']" % escape(name))
    else:
        print(">>> Start test suite: %s" % name)


def end_test_suite(name):
    if in_teamcity:
        print("##teamcity[testSuiteFinished name='%s']" %  escape(name))
    else:
        print(">>> End test suite: %s" % name)


def get_test_result_class():
    if not in_teamcity:
        return None
    return TeamCityTestResult
