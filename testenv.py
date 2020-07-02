import os, unittest

env_teamcity = 'TEST_IN_TEAMCITY'

in_teamcity = os.environ.get(env_teamcity)

def _tc_escape(s):
    s = s.replace("|", "||")
    s = s.replace("\n", "|n")
    s = s.replace("\r", "|r")
    s = s.replace("'", "|'")
    s = s.replace("[", "|[")
    s = s.replace("]", "|]")
    return s

class TeamCityTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def startTest(self, test):
        print("##teamcity[testStarted name='%s']" % _tc_escape(str(test)))
        return super().startTest(test)

    def stopTest(self, test):
        print("##teamcity[testFinished name='%s']" % _tc_escape(str(test)))
        return super().stopTest(test)

    def addError(self, test, err):
        print("##teamcity[testFailed name='%s' message='%s' details='%s']" % (_tc_escape(str(test)), _tc_escape(str(err[1])), _tc_escape(str(err[2]))))
        return super().addError(test, err)

    def addFailure(self, test, err):
        print("##teamcity[testFailed name='%s' message='%s' details='%s']" % (_tc_escape(str(test)), _tc_escape(str(err[1])), _tc_escape(str(err[2]))))
        return super().addFailure(test, err)

    def addSkip(self, test, reason):
        print("##teamcity[testIgnored name='%s' message='%s']" % (_tc_escape(str(test)), _tc_escape(str(reason))))
        return super().addSkip(test, reason)


def begin_test_suite(name):
    if in_teamcity:
        print("##teamcity[testSuiteStarted name='%s']" % _tc_escape(name))
    else:
        print(">>> Start test suite: %s" % name)


def end_test_suite(name):
    if in_teamcity:
        print("##teamcity[testSuiteFinished name='%s']" %  _tc_escape(name))
    else:
        print(">>> End test suite: %s" % name)


def get_test_result_class():
    if not in_teamcity:
        return None
    return TeamCityTestResult

