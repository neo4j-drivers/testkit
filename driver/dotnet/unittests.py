
"""
Executed in Go driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""
import os, subprocess

root_package = "github.com/neo4j/neo4j-dotnet-driver/neo4j"

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)

if __name__ == "__main__":

    # run the dotnet test framework from the neo4j.driver/neo4j.driver.tests directory.
    wd = os.getcwd()
    os.chdir("Neo4j.Driver/Neo4j.Driver.Tests")
    run(["dotnet", "test", "Neo4j.Driver.Tests.csproj"])
    os.chdir(wd)
