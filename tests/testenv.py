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
    """Parse an "index/count" shard spec into a 1-based (index, count)."""
    parts = [part.strip() for part in spec.split("/")]
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ValueError(
            'shard must look like "index/count", for example "2/5", got "%r"'
            % (spec,)
        )
    index, count = (int(part) for part in parts)
    if count < 1:
        raise ValueError("shard count must be at least 1, got %r" % (spec,))
    if not 1 <= index <= count:
        raise ValueError(
            "shard index must be between 1 and %d, got %r" % (count, spec)
        )
    return index, count


def shard_suite(suite, spec):
    # Tests are taken round-robin so tests from an expansive
    # module are spread across shards.
    index, count = parse_shard(spec)
    tests = list(_flatten(suite))
    selected = tests[index - 1::count]
    print(
        "Shard %d/%d: running %d of %d tests"
        % (index, count, len(selected), len(tests))
    )
    return unittest.TestSuite(selected)
