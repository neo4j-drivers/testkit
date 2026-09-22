import copy

from nutkit import protocol as types
from tests.shared import TestkitTestCase


class _TestSummaryBase(TestkitTestCase):
    """Test result summary contents."""

    def assert_plan_equal(self, actual, expected):
        missing_stats_detectable = self.driver_supports_features(
            types.Feature.API_SUMMARY_PROFILE_OPTIONAL_STATS
        )

        def adjust_expected(actual_child, expected_child):
            for key in [k for k, v in expected_child.items() if v is None]:
                if key not in actual_child:
                    expected_child.pop(key)
            for key in (k for k, v in actual_child.items() if v is None):
                expected_child.setdefault(key, None)

            if not missing_stats_detectable:
                for key, actual_value in actual_child.items():
                    if actual_value == 0 and expected_child.get(key) is None:
                        expected_child[key] = 0

            # drivers are free to represent lack of children with either:
            #   * an empty list
            #   * a null value
            #   * lack of the key
            expected_children = expected_child.get("children")
            expected_has_no_children = expected_children in (None, [])
            actual_children = actual_child.get("children")
            actual_has_no_children = actual_children in (None, [])

            if expected_has_no_children and actual_has_no_children:
                if "children" in actual_child:
                    expected_child["children"] = actual_children
                else:
                    expected_child.pop("children", None)
                return

            expected_children = expected_children or []
            actual_children = actual_children or []
            if len(expected_children) == len(actual_children):
                for ac, ec in zip(actual_children, expected_children):
                    adjust_expected(ac, ec)

        actual = copy.deepcopy(actual)
        expected = copy.deepcopy(expected)
        adjust_expected(actual, expected)
        self.assertEqual(actual, expected)
