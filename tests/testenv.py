from teamcity import (
    in_teamcity,
    team_city_test_result,
    test_kit_basic_test_result,
)


def get_test_result_class(name):
    if not in_teamcity:
        return test_kit_basic_test_result(name)
    return team_city_test_result(name)
