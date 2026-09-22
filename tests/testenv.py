import unittest
from collections import Counter

from teamcity import (
    in_teamcity,
    team_city_test_result,
    test_kit_basic_test_result,
)


def get_test_result_class(name):
    if not in_teamcity:
        return test_kit_basic_test_result(name)
    return team_city_test_result(name)


def _flatten(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _flatten(item)
        else:
            yield item


def assert_no_duplicate_tests(suite):
    counts = Counter(test.id() for test in _flatten(suite))
    duplicates = {name: count for name, count in counts.items() if count > 1}
    if not duplicates:
        return

    listing = "\n".join(
        f"    {count}x {name}"
        for name, count in sorted(duplicates.items())
    )
    raise ValueError(
        f"{sum(duplicates.values()) - len(duplicates)} tests are collected "
        f"more than once. A test class bound as an attribute of another test "
        f"module is collected there too, so import the module rather than the "
        f"class when subclassing:\n" + listing
    )
