import unittest

from .env import in_teamcity


def escape(s):
    s = s.replace("|", "||")
    s = s.replace("\n", "|n")
    s = s.replace("\r", "|r")
    s = s.replace("'", "|'")
    s = s.replace("[", "|[")
    s = s.replace("]", "|]")
    return s


def test_kit_basic_test_result(name):
    class TestKitBasicTestResult(unittest.TextTestResult):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def startTestRun(self):  # noqa: N802
            if in_teamcity:
                self.stream.writeln("##teamcity[testSuiteStarted name='%s']"
                                    % escape(name))
            else:
                self.stream.writeln(">>> Start test suite: %s" % name)
            self.stream.flush()

        def stopTestRun(self):  # noqa: N802
            if in_teamcity:
                self.stream.writeln("##teamcity[testSuiteFinished name='%s']"
                                    % escape(name))
            else:
                self.stream.writeln(">>> End test suite: %s" % name)
            self.stream.flush()

    return TestKitBasicTestResult


def team_city_test_result(name):
    base = test_kit_basic_test_result(name)

    class TeamCityTestResult(base):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def startTest(self, test):  # noqa: N802  # noqa: N802
            self.stream.writeln("##teamcity[testStarted name='%s']"
                                % escape(str(test)))
            self.stream.flush()
            super().startTest(test)

        def stopTest(self, test):  # noqa: N802
            super().stopTest(test)
            self.stream.writeln("##teamcity[testFinished name='%s']\n"
                                % escape(str(test)))
            self.stream.flush()

        def addError(self, test, err):  # noqa: N802
            self.stream.writeln(
                "##teamcity[testFailed name='%s' message='%s' details='%s']"
                % (escape(str(test)), escape(str(err[1])), escape(str(err[2])))
            )
            self.stream.flush()
            super().addError(test, err)
            self.stream.write("%s" % self.errors[-1][1])
            self.stream.flush()

        def addFailure(self, test, err):  # noqa: N802
            self.stream.writeln(
                "##teamcity[testFailed name='%s' message='%s' details='%s']"
                % (escape(str(test)), escape(str(err[1])), escape(str(err[2])))
            )
            self.stream.flush()
            super().addFailure(test, err)
            self.stream.write("%s" % self.failures[-1][1])
            self.stream.flush()

        def addSkip(self, test, reason):  # noqa: N802
            self.stream.writeln(
                "##teamcity[testIgnored name='%s' message='%s']"
                % (escape(str(test)), escape(str(reason)))
            )
            self.stream.flush()
            super().addSkip(test, reason)

        def printErrors(self):  # noqa: N802
            # errors and failures are printed already at the end of the tests
            pass

    return TeamCityTestResult
