import unittest

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


def parse_shard(spec):
    parts = [part.strip() for part in spec.split("/")]
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ValueError('shard must be in the form "#/#"')

    index, count = (int(part) for part in parts)
    if count < 1:
        raise ValueError("shard count must be at least 1")

    if not 1 <= index <= count:
        raise ValueError("shard index must be >= 1 and <= %d" % (count,))

    return index, count


def shard_list(items, spec):
    index, count = parse_shard(spec)
    return items[index - 1::count]


def shard_suite(suite, spec):
    tests = list(_flatten(suite))
    selected = shard_list(tests, spec)
    print(
        "Shard %s: running %d of %d tests"
        % (spec.strip(), len(selected), len(tests))
    )
    return unittest.TestSuite(selected)
