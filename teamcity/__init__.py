import os

from teamcity.download import DockerImage
from teamcity.testresult import (
    TeamCityTestResult,
    escape,
)


def evaluate_env_variable():
    env = os.environ.get('TEST_IN_TEAMCITY', 'False').upper()
    return env == 'TRUE' or env == '1'


in_teamcity = evaluate_env_variable()
