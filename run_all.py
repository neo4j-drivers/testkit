import os
import sys
import subprocess
import tempfile

drivers = [
    {
        "name": "go",
        "repo": "git@github.com:neo4j/neo4j-go-driver.git"
    },
    {
        "name": "javascript",
        "repo": "git@github.com:neo4j/neo4j-javascript-driver.git"
    },
    {
        "name": "python",
        "repo": "git@github.com:neo4j/neo4j-python-driver.git"
    },
    {
        "name": "java",
        "repo": "git@github.com:neo4j/neo4j-java-driver.git"
    },
    {
        "name": "dotnet",
        "repo": "git@github.com:neo4j/neo4j-dotnet-driver.git"
    }
]


def setup_environment():
    temp_path = tempfile.gettempdir()
    driver_repo_path = os.path.join(temp_path, 'driver')

    # cleanup environment
    os.makedirs(temp_path, exist_ok=True)
    rmdir(driver_repo_path)

    branch = os.environ.get("TEST_DRIVER_BRANCH", "4.3")

    return (temp_path, driver_repo_path, branch)


def rmdir(dir):
    try:
        subprocess.run(['rm', '-fr', dir])
    except FileNotFoundError as error:
        pass


def clone_repo(driver, branch, path):
    subprocess.run(["git", "clone", "--branch", branch,
                    driver.get("repo"), path], check=True)


def update_environment(driver, repo_path):
    os.environ['TEST_DRIVER_REPO'] = os.path.abspath(repo_path)
    os.environ['TEST_DRIVER_NAME'] = driver.get('name')


def run():
    arguments = sys.argv[1:]
    subprocess.run(["python3", "main.py"] + arguments, check=True)


if __name__ == "__main__":
    (temp_path, driver_repo_path, branch) = setup_environment()

    for driver in drivers:
        clone_repo(driver, branch, driver_repo_path)
        update_environment(driver, driver_repo_path)
        run()
        rmdir(driver_repo_path)

    rmdir(temp_path)
