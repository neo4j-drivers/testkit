import os, sys, atexit, subprocess, time

from test import a_test
from nutkit.backend import Backend


containers = ["driver", "neo4jserver"]
networks = ["the-bridge"]


def cleanup():
    for c in containers:
        subprocess.run(["docker", "rm", "-f", "-v", c],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for n in networks:
        subprocess.run(["docker", "network", "rm", n],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    # Retrieve needed parameters from environment
    driverRepo = os.environ.get('NUT_DRIVER_REPO')
    driverImage = os.environ.get('NUT_DRIVER_IMAGE')
    nutRepo = os.environ.get('NUT_NUT_REPO')
    buildRoot = os.environ.get('NUT_BUILD_ROOT')

    if not driverRepo or not nutRepo or not driverImage or not buildRoot:
        print("Missing environment variables")
        sys.exit(1)

    # Important to stop all docker images upon exit
    atexit.register(cleanup)

    # Also make sure that none of those images are running at this point
    cleanup()

    # Create network to be shared among the instances
    args = [
        "docker", "network", "create", "the-bridge"
    ]
    subprocess.run(args)

    # Bootstrap the driver docker image by running a bootstrap script in the image.
    # The driver docker image only contains the tools needed to build, not the built driver.
    args = [
        "docker", "run",
        # Bootstrap script is in the repo containing this script mounted as /nutkit
        "-v", "%s:/nutkit" % nutRepo,
        # The driver repo mounted as /driver
        "-v", "%s:/driver" % driverRepo,
        # Name of the docker container
        "--name", "driver",
        # Set working folder to the driver
        "-w", "/driver",
        # Remove itself upon exit
        "--rm",
        "--net=the-bridge",
        # Name of the image
        driverImage,
        # Bootstrap command
        "python3", "/nutkit/driver_bootstrap.py"
    ]
    p = subprocess.Popen(args, bufsize=0, encoding='utf-8', stdout=subprocess.PIPE)
    print("Waiting for driver container to start")
    line = p.stdout.readline()
    if line.strip() != "ok":
        print(line)
        sys.exit(2)
    print("Driver container started")


    # Build the driver and it's nutkit backend
    print("Build nutkit backend in driver container")
    subprocess.run([
        "docker", "exec",
        "--env", "BUILD_ROOT=%s" % buildRoot,
        "driver",
        "python3", "/nutkit/driver_go_build.py"
    ])
    print("Finished building driver")



    # Start a Neo4j server
    print("Starting neo4j server")
    args = [
        "docker", "run",
        # Name of the docker container
        "--name", "neo4jserver",
        # Remove itself upon exit
        "--rm",
        # Run in background
        "--detach",
        "--env", "NEO4J_dbms_connector_bolt_advertised__address=neo4jserver:7687",
        "--net=the-bridge",
        # Force a password
        "--env", "NEO4J_AUTH=%s/%s" % ("neo4j", "pass"),
        # Image
        "neo4j:latest",
    ]
    subprocess.run(args)


    # The nutbackend is invoked with the following docker command, need to use interactive
    # to get stdin forwarded.
    args = [
        "docker", "exec",
        "--interactive",
        "driver",
        os.path.join(buildRoot, "nutbackend")
    ]
    backend = Backend(args)

    # Wait until server is listening before running tests
    time.sleep(10)
    print("Neo4j server started")

    print("Running tests on server...")
    a_test(backend)
    print("Done running tests")


