
"""
Executed in dotnet driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""
import os, subprocess


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":

    # run the dotnet integration test framework from the neo4j.driver/neo4j.driver.tests.integration directory.
    # this should not be required once all tests are moved over...
    wd = os.getcwd()
    os.chdir("Neo4j.Driver/Neo4j.Driver.Tests.Integration")

    # This generates a bit ugly output when not in TeamCity, it can be fixed by checking the TEST_IN_TEAMCITY
    # environment flag...(but needs to be passed to the container somehow)
    os.environ.update({"TEAMCITY_PROJECT_NAME": "integrationtests"})

    try:
        run(["dotnet", "test", "Neo4j.Driver.Tests.Integration.csproj", "--filter",
             "DisplayName~IntegrationTests.Internals"])
    except Exception as e:
        print("Failed 1 or more tests in IntegrationTests.Internals")

    try:
        run(["dotnet", "test", "Neo4j.Driver.Tests.Integration.csproj", "--filter", "DisplayName~IntegrationTests.Direct"])
    except Exception as e:
        print("Failed 1 or more tests in IntegrationTests.Direct")

    try:
        run(["dotnet", "test", "Neo4j.Driver.Tests.Integration.csproj", "--filter", "DisplayName~IntegrationTests.Reactive"])
    except Exception as e:
        print("Failed 1 or more tests in IntegrationTests.Reactive")

    try:
        run(["dotnet", "test", "Neo4j.Driver.Tests.Integration.csproj", "--filter", "DisplayName~IntegrationTests.Routing"])
    except Exception as e:
        print("Failed 1 or more tests in IntegrationTests.Routing")

    try:
        run(["dotnet", "test", "Neo4j.Driver.Tests.Integration.csproj", "--filter", "DisplayName~IntegrationTests.Types"])
    except Exception as e:
        print("Failed 1 or more tests in IntegrationTests.Types")

    try:
        run(["dotnet", "test", "Neo4j.Driver.Tests.Integration.csproj", "--filter", "DisplayName~Examples"])
    except Exception as e:
        print("Failed 1 or more tests in IntegationTests.Examples")

    os.chdir(wd)
