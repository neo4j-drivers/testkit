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


class Report:
    def __init__(self, test, parent_test=None):
        self.test = test
        self.parent_test = parent_test
        self.errors = []
        self.failures = []
        self.unexpected_successes = []
        self.skipped = []


def team_city_test_result(name):
    base = test_kit_basic_test_result(name)

    class TeamCityTestResult(base):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._last_error_count = 0
            self._last_failure_count = 0
            self._last_unexpected_successes_count = 0
            self._last_skipped_count = 0

        def startTest(self, test):  # noqa: N802  # noqa: N802
            self.stream.writeln("##teamcity[testStarted name='%s']"
                                % escape(str(test)))
            self.stream.flush()
            self._last_error_count = len(self.errors)
            self._last_failure_count = len(self.failures)
            self._last_unexpected_successes_count = \
                len(self.unexpectedSuccesses)
            self._last_skipped_count = len(self.skipped)
            super().startTest(test)

        def stopTest(self, test):  # noqa: N802
            super().stopTest(test)
            new_errors = self.errors[self._last_error_count:]
            new_failures = self.failures[self._last_failure_count:]
            new_unexpected_successes = self.unexpectedSuccesses[
                self._last_unexpected_successes_count:
            ]
            new_skipped = self.skipped[self._last_skipped_count:]
            self._last_error_count = len(self.errors)
            self._last_failure_count = len(self.failures)
            self._last_unexpected_successes_count = \
                len(self.unexpectedSuccesses)
            self._last_skipped_count = len(self.skipped)

            test_reports = {str(test): Report(test)}
            for sub_test, error in new_errors:
                key = str(sub_test)
                if key not in test_reports:
                    test_reports[key] = Report(sub_test, parent_test=test)
                test_reports[key].errors.append(error)
            for sub_test, failure in new_failures:
                key = str(sub_test)
                if key not in test_reports:
                    test_reports[key] = Report(sub_test, parent_test=test)
                test_reports[key].failures.append(failure)
            for sub_test, unexpected_successes in new_unexpected_successes:
                key = str(sub_test)
                if key not in test_reports:
                    test_reports[key] = Report(sub_test, parent_test=test)
                test_reports[key].unexpected_successes.append(
                    unexpected_successes
                )
            for sub_test, reason in new_skipped:
                key = str(sub_test)
                if key not in test_reports:
                    test_reports[key] = Report(sub_test, parent_test=test)
                test_reports[key].skipped.append(reason)

            self.stream.writeln(
                self.format_report(test_reports.pop(str(test)))
            )
            self.stream.writeln("##teamcity[testFinished name='%s']\n"
                                % escape(str(test)))
            for report in test_reports.values():
                self.stream.writeln("##teamcity[testStarted name='%s']"
                                    % escape(str(report.test)))
                self.stream.writeln(self.format_report(report))
                self.stream.writeln("##teamcity[testFinished name='%s']\n"
                                    % escape(str(report.test)))
            self.stream.flush()

        def printErrors(self):  # noqa: N802
            # errors and failures are printed already at the end of the tests
            pass

        def format_report(self, report):
            test = report.test
            res = ""
            if report.errors or report.failures or report.unexpected_successes:
                details = ""
                if report.errors:
                    details += (
                        self.format_error_list(test, "ERROR", report.errors)
                        + "\n"
                    )
                if self.failures:
                    details += (
                        self.format_error_list(test, "FAIL", report.failures)
                        + "\n"
                    )
                if report.unexpected_successes:
                    details += (
                        self.format_error_list(test,
                                               "UNEXPECTED SUCCESS",
                                               report.unexpected_successes)
                        + "\n"
                    )
                if report.parent_test is not None:
                    details += (
                        "For stdout and stderr see parent test: %s\n"
                        % str(report.parent_test)
                    )
                problems = (report.errors + report.failures
                            + report.unexpected_successes)
                res += (
                    "##teamcity[testFailed name='%s' "
                    "message='Failed (%s problem(s))' details='%s']"
                    % (escape(str(test)), escape(str(len(problems))),
                       escape(details))
                )
            elif report.skipped:
                res += (
                    "##teamcity[testIgnored name='%s' message='%s']"
                    % (escape(str(test)),
                       escape("\n".join(map(str, report.skipped))))
                )
            return res

        def format_error_list(self, test, flavour, errors):
            s = ""
            for err in errors:
                s += self.separator1 + "\n"
                s += "%s: %s\n" % (flavour, self.getDescription(test))
                s += self.separator2 + "\n"
                s += "%s\n" % err
            return s

    return TeamCityTestResult
