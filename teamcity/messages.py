from contextlib import contextmanager

from .env import in_teamcity

__all__ = [
    "test_suite_started_message",
    "test_suite_started",
    "test_suite_finished_message",
    "test_suite_finished",
    "test_suite",
    "test_started_message",
    "test_started",
    "test_finished_message",
    "test_finished",
    "test_failed_message",
    "test_failed",
    "test_ignored_message",
    "test_ignored",
]


def _escape(s):
    s = str(s)
    s = s.replace("|", "||")
    s = s.replace("\n", "|n")
    s = s.replace("\r", "|r")
    s = s.replace("'", "|'")
    s = s.replace("[", "|[")
    s = s.replace("]", "|]")
    return s


def test_suite_started_message(name):
    return f"##teamcity[testSuiteStarted name='{_escape(name)}']"


def test_suite_started(name):
    if in_teamcity:
        print(test_suite_started_message(name), flush=True)


def test_suite_finished_message(name):
    return f"##teamcity[testSuiteFinished name='{_escape(name)}']"


def test_suite_finished(name):
    if in_teamcity:
        print(test_suite_finished_message(name), flush=True)


@contextmanager
def test_suite(name):
    test_suite_started(name)
    try:
        yield
    finally:
        test_suite_finished(name)


def test_started_message(name):
    return f"##teamcity[testStarted name='{_escape(name)}']"


def test_started(name):
    if in_teamcity:
        print(test_started_message(name), flush=True)


def test_finished_message(name):
    return f"##teamcity[testFinished name='{_escape(name)}']"


def test_finished(name):
    if in_teamcity:
        print(test_finished_message(name), flush=True)


def test_failed_message(name, message=None):
    if message:
        return (
            f"##teamcity[testFailed name='{_escape(name)}' "
            f"message='{_escape(message)}']"
        )
    else:
        return f"##teamcity[testFailed name='{_escape(name)}']"


def test_failed(name, message=None):
    if in_teamcity:
        print(test_failed_message(name, message), flush=True)


def test_ignored_message(name, message=None):
    if message:
        return (
            f"##teamcity[testIgnored name='{_escape(name)}' "
            f"message='{_escape(message)}']"
        )
    else:
        return f"##teamcity[testIgnored name='{_escape(name)}']"


def test_ignored(name, message=None):
    if in_teamcity:
        print(test_ignored_message(name, message), flush=True)
