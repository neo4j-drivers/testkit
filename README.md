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
    Name of testkit branch. When running locally this defaults to 'local'.
  * TEST_BUILD_CACHE_ENABLED
    Set to `true` to enable build cache persistence via Docker Volumes for supported build systems. Only Maven is supported at the moment and it stores its data in `testkit-m2` volume.

```console
export TEST_DRIVER_NAME=go
export TEST_DRIVER_REPO=/home/clones/neo4j/neo4j-go-driver
python3 main.py
```

## Local development

### Running a subset of tests or configurations

When running testkit locally from the command line you can specify which test types you want to run and what
configurations you want to run against. For example:
```console
python3 main.py --tests TESTKIT_TESTS UNIT_TESTS --configs 4.0-community 4.1-enterprise
```

To see a list of available test types and configurations use:

```
python3 main.py --help
```

### Running tests against backend on host

While developing the driver it is useful and much faster to manually build and start the drivers testkit
backend and run integration or stub tests against that. For this setup no Docker containters are needed.

To run integration tests you need to:
  * Provide the tests with a running Neo4j instance. This instance can be running locally (Jar or Docker)
    or on a server somewhere (be careful, the tests might destroy data).

    Example on how to start latest Neo4j server locally in Docker:
    ```console
    docker run --name neo4j --env NEO4J_AUTH=neo4j/pass -p7687:7687 --rm neo4j:latest
    ```

    For security reasonse there are NO DEFAULT settings for the Neo4j host that the tests are running
    against. The environment variable TEST_NEO4J_HOST needs to be set to the correct location.

  * Start the drivers testkit backend.
    The framework defaults to connect to port 9876 on localhost. If the backend is running on another
    host or port the environment variables TEST_BACKEND_HOST and TEST_BACKEND_PORT needs to be set
    in the environment where the tests are invoked.

  * Run the integration tests using standard Python unittest syntax. The integration tests are all
    prefixed with tests.neo4j.XXX. Where XXX can be a single Python file (without the .py), a class
    in the single Python file or a single test.

    To run a single named test using a local Neo4j database:
    ```console
    export TEST_NEO4J_HOST=localhost
    python3 -m unittest tests.neo4j.datatypes.TestDataTypes.testShouldEchoBack
    ```

Running stub tests locally is simpler than running the integration tests:
  * Start the drivers testkit backend, see above.
  * Run the stub tests same way as the integration tests but they are rooted at
    tests.stub instead of tests.neo4j

Environment variables used to control how tests are executed:
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
All of these variables are normally set by the main runner.

## Running all test suites for all known drivers within docker containers 

This test runner will clone and run the tests for each known driver repository. 

Requirements on host:

  * Python3.6 >=
  * Docker 19.03 >=

Environment variables:
  * TEST_DRIVER_BRANCH
    Branch to be tested in all drivers. Default: 4.3

```console
python3 run_all.py
```

This test runs the `main.py` overriding the enviroment variables `TEST_DRIVER_NAME` and `TEST_DRIVER_REPO` with correct values for each driver. The others enviroment variables will be used by `main.py` as usual.

## Running stress test suite against a running Neo4j instance

This test runner will build the driver and its testkit backend, setup the environment
and invoke the driver native stress test suite.

Environment variables:
  * TEST_NEO4J_URI
    Full URI for connecting to running Neo4j instance, for example:
      neo4j+s://somewhere.com:7687
  * TEST_NEO4J_USER
    Username used to connect, defaults to neo4j
  * TEST_NEO4J_PASS
    Password used to connect

