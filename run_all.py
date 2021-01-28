import os
import sys
import subprocess
import tempfile

drivers = [
    {
        "name": "go",
        "repo": "https://github.com/neo4j/neo4j-go-driver.git",
        "art": [
            " XXX   XX ",
            "X     X  X",
            "X XX  X  X",
            "X  X  X  X",
            "X  X  X  X",
            " XX    XX ",
        ],
    },
    {
        "name": "javascript",
        "repo": "https://github.com/neo4j/neo4j-javascript-driver.git",
        "art": [
            "   X   XXX",
            "   X  X   ",
            "   X  XXX ",
            "X  X     X",
            "X  X     X",
            " XX   XXX ",
        ],
    },
    {
        "name": "python",
        "repo": "https://github.com/neo4j/neo4j-python-driver.git",
        "art": [
            "XXX   X  X",
            "X  X  X  X",
            "X  X  X  X",
            "X X    XX ",
            "X      XX ",
            "X      XX ",
        ],
    },
    {
        "name": "java",
        "repo": "https://github.com/neo4j/neo4j-java-driver.git",
        "art": [
            "   X   XX ",
            "   X  X  X",
            "   X  XXXX",
            "X  X  X  X",
            "X  X  X  X",
            " XX   X  X",
        ],
    },
    {
        "name": "dotnet",
        "repo": "https://github.com/neo4j/neo4j-dotnet-driver.git",
        "art": [
            "    X    X",
            "    XX   X",
            "    X X  X",
            "    X   XX",
            " XX X    X",
            " XX X    X",
        ],
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

def print_art(driver, scale):
    print('')
    print('Testing %s (%s)' % (driver["name"], driver["repo"]))
    print('')

    art = driver.get("art", False)
    if not art:
        return

    print('')
    for l in art:
        for s in range(scale):
            for c in l:
                print(c*scale, end='')
            print('')
    print('')


if __name__ == "__main__":
    (temp_path, driver_repo_path, branch) = setup_environment()

    for driver in drivers:
        print_art(driver, 2)
        clone_repo(driver, branch, driver_repo_path)
        update_environment(driver, driver_repo_path)
        run()
        rmdir(driver_repo_path)

    rmdir(temp_path)
