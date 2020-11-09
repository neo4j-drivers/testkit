import os
from teamcity.download import DockerImage
from teamcity.testresult import TeamCityTestResult, escape


in_teamcity = os.environ.get('TEST_IN_TEAMCITY', False)
