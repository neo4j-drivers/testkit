"""
Runs all test suites in docker containers.

Relies on the following environment variables:

NUT_DRIVER_REPO     Path to root of driver git repository
NUT_NUTKIT_REPO     Path to root of nutkit git repository
NUT_DRIVER_IMAGE    Name of docker image that can be used to build the driver and host the
                    nut backend
NUT_BUILD_ROOT      Path on the driver image where build output is to be placed, the drivers
                    nut backend should be placed here and be named 'nutbackend'
"""

import os, sys, atexit, subprocess, time, unittest

import tests.neo4j.suites as suites


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
    nutRepo = os.environ.get('NUT_NUTKIT_REPO')
    driverImage = os.environ.get('NUT_DRIVER_IMAGE')
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
        # Expose backend on this port
        "-p9876:9876",
        # Set working folder to the driver
        "-w", "/driver",
        # Remove itself upon exit
        "--rm",
        "--net=the-bridge",
        # Name of the image
        driverImage,
        # Bootstrap command
        "python3", "/nutkit/driver/bootstrap.py"
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
        "python3", "/nutkit/driver/build_go.py"
    ])
    print("Finished building driver")

    print("Start backend in driver container")
    subprocess.run([
        "docker", "exec",
        "--detach",
        "driver",
        os.path.join(buildRoot, "nutbackend")
    ])
    print("Started backend in driver container")

    # Start a Neo4j server
    neo4jserver = "neo4jserver"
    print("Starting neo4j server")
    subprocess.run([
        "docker", "run",
        # Name of the docker container
        "--name", neo4jserver,
        # Remove itself upon exit
        "--rm",
        # Run in background
        "--detach",
        "--env", "NEO4J_dbms_connector_bolt_advertised__address=%s:7687" % neo4jserver,
        "--net=the-bridge",
        # Force a password
        "--env", "NEO4J_AUTH=%s/%s" % ("neo4j", "pass"),
        # Image
        "neo4j:latest",
    ])

    # Wait until server is listening before running tests
    time.sleep(10)
    print("Neo4j server started")

    # Make sure that the tests instruct the driver to connect to neo4jserver docker container
    os.environ['NUT_NEO4J_HOST'] = neo4jserver

    print("Running tests on server...")
    runner = unittest.TextTestRunner()
    runner.run(suites.single_community)
    print("Done running tests")

