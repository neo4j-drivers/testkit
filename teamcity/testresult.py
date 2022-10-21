# import time
import unittest


def escape(s):
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

    def startTest(self, test):  # noqa: N802  # noqa: N802
        print("##teamcity[testStarted name='%s']" % escape(str(test)))
        return super().startTest(test)

    def stopTest(self, test):  # noqa: N802
        print("##teamcity[testFinished name='%s']\n" % escape(str(test)))
        return super().stopTest(test)

    def addError(self, test, err):  # noqa: N802
        print("##teamcity[testFailed name='%s' message='%s' details='%s']"
              % (escape(str(test)), escape(str(err[1])), escape(str(err[2]))))
        # time.sleep(0.5)
        return super().addError(test, err)

    def addFailure(self, test, err):  # noqa: N802
        print("##teamcity[testFailed name='%s' message='%s' details='%s']"
              % (escape(str(test)), escape(str(err[1])), escape(str(err[2]))))
        # time.sleep(0.5)
        return super().addFailure(test, err)

    def addSkip(self, test, reason):  # noqa: N802
        print("##teamcity[testIgnored name='%s' message='%s']"
              % (escape(str(test)), escape(str(reason))))
        return super().addSkip(test, reason)
