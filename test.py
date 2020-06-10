
from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken, NullRecord
from nutkit.protocol import CypherString


if __name__ == "__main__":
    # Start backend
    backend = Backend(["/home/peter/code/neo4j/neo4j-go-driver/nutbackend/nutbackend"])

    # Example test case
    authToken = AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")
    driver = Driver(backend, "bolt://localhost:9999", authToken)
    session = driver.session("r", ["bm1"])
    result = session.run("RETURN NULL AS nullcol, 1 AS intcol, [1, 'a'] AS arrcol ")
    while True:
        record = result.next()
        if isinstance(record, NullRecord):
            break
        for v in record.values:
            print("Record: "+str(v))


    result = session.run("MATCH (n:X {txt: $txt}) RETURN n", {'txt': CypherString('hello')})
    while True:
        record = result.next()
        if isinstance(record, NullRecord):
            break
        for v in record.values:
            print("Record: "+str(v))

    def retryableRead(tx):
        result = tx.run("MATCH (n:X {txt: $txt}) RETURN n", {'txt': CypherString('hello')})
        record = result.next()
        return record.values

    r = session.readTransaction(retryableRead)
    print(r)
