import os
import subprocess
import sys
import tempfile
import traceback

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
        "branch-translation": {
            "4.0": "4.2",
            "4.1": "4.2",
        }
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
        "branch-translation": {
        }
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
        "name": "java-async",
        "repo": "https://github.com/neo4j/neo4j-java-driver.git",
        "art": [
            "   X   XX       XX    XXX  X  X  X  X   CCC",
            "   X  X  X     X  X  X     X  X  XX X  CC  ",
            "   X  XXXX     XXXX  XXX   X  X  X XX  C   ",
            "X  X  X  X     X  X     X   XX   X XX  C   ",
            "X  X  X  X     X  X     X   XX   X  X  CC  ",
            " XX   X  X     X  X  XXX    XX   X  X   CCC",
        ],
        "extra-env": {
            "TEST_BACKEND_SERVER": "async"
        },
    },
    {
        "name": "java-reactive",
        "repo": "https://github.com/neo4j/neo4j-java-driver.git",
        "art": [
            "   X   XX      XXX   X  X",
            "   X  X  X     X  X  X  X",
            "   X  XXXX     XXX    XX ",
            "X  X  X  X     X XX  X  X",
            "X  X  X  X     X  X  X  X",
            " XX   X  X     X  X  X  X",
        ],
        "extra-env": {
            "TEST_BACKEND_SERVER": "reactive"
        },
    },
    {
        "name": "dotnet",
        "repo": "https://github.com/neo4j/neo4j-dotnet-driver.git",
        "art": [
            "    X  X",
            "    XX X",
            "    X XX",
            "    X XX",
            " XX X  X",
            " XX X  X",
        ],
        "branch-translation": {
            # TODO: until a 4.4 branch has been created
            "4.4": "4.3",
        }
    }
]


def patched_process_run(*popenargs, input=None, capture_output=False,
                        timeout=None, check=False, **kwargs):
    """
    Copy of subprocess.run with increased process._sigint_wait_secs.

    This gives testkit enough time to clean the docker "mess" it made.
    """
    if input is not None:
        if kwargs.get('stdin') is not None:
            raise ValueError('stdin and input arguments may not both be used.')
        kwargs['stdin'] = subprocess.PIPE

    if capture_output:
        if kwargs.get('stdout') is not None or kwargs.get('stderr') is not None:
            raise ValueError('stdout and stderr arguments may not be used '
                             'with capture_output.')
        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE

    with subprocess.Popen(*popenargs, **kwargs) as process:
        process._sigint_wait_secs = 10
        try:
            stdout, stderr = process.communicate(input, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            if subprocess._mswindows:
                # Windows accumulates the output in a single blocking
                # read() call run on child threads, with the timeout
                # being done in a join() on those threads.  communicate()
                # _after_ kill() is required to collect that and add it
                # to the exception.
                exc.stdout, exc.stderr = process.communicate()
            else:
                # POSIX _communicate already populated the output so
                # far into the TimeoutExpired exception.
                process.wait()
            raise
        except:  # Including KeyboardInterrupt, communicate handled that.
            process.kill()
            # We don't call process.wait() as .__exit__ does that for us.
            raise
        retcode = process.poll()
        if check and retcode:
            raise subprocess.CalledProcessError(retcode, process.args,
                                                output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(process.args, retcode, stdout, stderr)


def setup_environment():
    temp_path = tempfile.gettempdir()
    driver_repo_path = os.path.join(temp_path, 'driver')

    # cleanup environment
    os.makedirs(temp_path, exist_ok=True)
    rmdir(driver_repo_path)

    branch = os.environ.get("TEST_DRIVER_BRANCH", "4.4")

    return driver_repo_path, branch


def rmdir(dir_):
    try:
        subprocess.run(['rm', '-fr', dir_])
    except FileNotFoundError:
        pass


def translate_branch(driver, branch):
    return driver.get("branch-translation", {}).get(branch, branch)


def clone_repo(driver, branch, path):
    subprocess.run(["git", "clone", "--branch", branch,
                    driver.get("repo"), path], check=True)


def update_environment(driver, repo_path):
    os.environ['TEST_DRIVER_REPO'] = os.path.abspath(repo_path)
    os.environ['TEST_DRIVER_NAME'] = driver.get('name')
    os.environ['ARTIFACTS_DIR'] = os.path.join(".", "artifacts",
                                               driver.get("name"))
    os.environ.update(driver.get("extra-env", {}))


def run():
    arguments = sys.argv[1:]
    try:
        patched_process_run(["python3", "main.py"] + arguments, check=True)
        return True
    except subprocess.CalledProcessError:
        if os.environ.get("TEST_RUN_ALL_DRIVERS", "").lower() \
                in ("true", "y", "yes", "1", "on"):
            traceback.print_exc()
            return False
        else:
            raise


def print_art(driver, branch, scale):
    print('')
    print(
        'Testing %s (%s) branch %s' % (driver["name"], driver["repo"], branch)
    )
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
    sys.stdout.flush()


def main():
    driver_repo_path, branch = setup_environment()
    success = True

    for driver in drivers:
        print_art(driver, branch, 2)
        translated_branch = translate_branch(driver, branch)
        try:
            clone_repo(driver, translated_branch, driver_repo_path)
            update_environment(driver, driver_repo_path)
            success = run() and success
        finally:
            rmdir(driver_repo_path)

    if not success:
        sys.exit("One or more drivers caused failing tests.")


if __name__ == "__main__":
    main()
