
from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken


if __name__ == "__main__":
    # Start backend
    #backend = Backend(["python3", "backendexample.py"])
    backend = Backend(["/home/peter/code/neo4j/neo4j-go-driver/nutbackend/nutbackend"])

    # Example test case
    authToken = AuthorizationToken(scheme="basic", principal="neo4j", credentials="neo4j")
    driver = Driver(backend, "bolt://localhost", authToken)
    session = driver.session("r", ["bm1"])
    result = session.run("cypher")
