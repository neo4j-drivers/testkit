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

    def startTest(self, test):
        print("##teamcity[testStarted name='%s']" % escape(str(test)))
        return super().startTest(test)

    def stopTest(self, test):
        print("##teamcity[testFinished name='%s']\n" % escape(str(test)))
        return super().stopTest(test)

    def addError(self, test, err):
        print("##teamcity[testFailed name='%s' message='%s' details='%s']" % (escape(str(test)), escape(str(err[1])), escape(str(err[2]))))
        return super().addError(test, err)

    def addFailure(self, test, err):
        print("##teamcity[testFailed name='%s' message='%s' details='%s']" % (escape(str(test)), escape(str(err[1])), escape(str(err[2]))))
        return super().addFailure(test, err)

    def addSkip(self, test, reason):
        print("##teamcity[testIgnored name='%s' message='%s']" % (escape(str(test)), escape(str(reason))))
        return super().addSkip(test, reason)

