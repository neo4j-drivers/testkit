# Neo4j drivers integration/conformance tests

## Running all test suites within docker containers

Requirements on host:
  * Python3.6 >=
  * Docker 19.03 >=

Environment variables:
  * TEST_DRIVER_NAME
    Set to the name of the driver in lowercase, should match any of the drivers in drivers folder.
  * TEST_DRIVER_REPO
    Path to driver repository
  * TEST_BRANCH
    Name of testkit branch. When running locally set this to 'local'.
  * TEST_BUILD_CACHE_ENABLED
    Set to `true` to enable build cache persistence via Docker Volumes for supported build systems. Only Maven is supported at the moment and it stores its data in `testkit-m2` volume.

```console
export TEST_DRIVER_NAME=go
export TEST_DRIVER_REPO=/home/clones/neo4j/neo4j-go-driver
python3 main.py
```

## Running single test or suite

Requirements on host:
  * Python 3.6 >=
  * A running Neo4j server for test, NO production data
  * A running test backend for the driver to test

Environment variables:
  * TEST_NEO4J_HOST
    Host or ip where Neo4j server is running.
    Should normally be set to localhost.
  * TEST_NEO4J_USER
    Username used to connect to Neo4j server.
    Defaults to 'neo4j'
  * TEST_NEO4J_PASS
    Password used to connect to Neo4j server.
    Defaults to 'pass'
  * TEST_NEO4J_PORT
    Defaults to Bolt port 7687, normally not needed.
  * TEST_BACKEND_HOST
    Defaults to localhost, normally not needed.
  * TEST_BACKEND_PORT
    Defaults to 9876, normally not needed.

To start latest Neo4j server locally in Docker:
Note that Docker is not needed, the database used for the test could be running on another machine
or as a host application.
```console
docker run --name neo4j --env NEO4J_AUTH=neo4j/pass -p7687:7687 --rm neo4j:latest
```

To run a single named test using a local Neo4j database:
```console
export TEST_NEO4J_HOST=localhost
python3 -m unittest tests.neo4j.datatypes.TestDataTypes.testShouldEchoBack
```
## Running all test suites for all known drivers within docker containers 

This test runner will clone and run the tests for each known driver repository. 

Requirements on host:

  * Python3.6 >=
  * Docker 19.03 >=

Environment Variables:
  * TEST_DRIVER_BRANCH
    Branch to be tested in all drivers. Default: 4.3

```console
python3 run_all.py
```

This test runs the `main.py` overriding the enviroment variables `TEST_DRIVER_NAME` and `TEST_DRIVER_REPO` with correct values for each driver. The others enviroment variables will be used by `main.py` as usual.

## Command Line Usage

When running testkit locally from the command line you can specify which test types you want to run and what
configurations you want to run against. For example:
```console
python3 main.py --tests TESTKIT_TESTS UNIT_TESTS --configs 4.0-community 4.1-enterprise
```

To see a list of available test types and configurations use:

```
python3 main.py --help
```
