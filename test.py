
from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken


if __name__ == "__main__":
    # Start backend
    #backend = Backend(["python3", "backendexample.py"])
    backend = Backend(["/home/peter/code/neo4j/neo4j-go-driver/nutbackend/nutbackend"])

    # Example test case
    authToken = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
    driver = Driver(backend, "bolt://localhost:9999", authToken)
    session = driver.session("r", ["bm1"])
    result = session.run("RETURN NULL AS nullcol, 1 AS intcol")
    record = result.next()
    print(record)
    print(record.values[0])
    print(record.values[1])
    record = result.next()
    print(record)
