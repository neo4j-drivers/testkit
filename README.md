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
