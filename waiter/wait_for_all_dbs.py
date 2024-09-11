import sys
import time

import neo4j
from neo4j.exceptions import (
    DriverError,
    Neo4jError,
)

TIMEOUT = 120
LAST_ERROR = ""


def check_availability(driver):
    global LAST_ERROR
    try:
        records, _, _ = driver.execute_query(
            "SHOW SERVERS YIELD *",
            database_="system",
        )
        if not records:
            LAST_ERROR = "No records returned"
            return False
        for record in records:
            hosting = record.get("hosting")
            requested_hosting = record.get("requestedHosting")
            if None in (hosting, requested_hosting):
                LAST_ERROR = "Missing hosting or requestedHosting"
                return False
            if hosting != requested_hosting:
                LAST_ERROR = "hosting != requestedHosting"
                return False
        return True
    except (DriverError, Neo4jError) as e:
        LAST_ERROR = str(e)
        return False


def main(host, port, user, password):
    url = f"bolt://{host}:{port}"
    auth = (user, password)
    t0 = time.perf_counter()

    with neo4j.GraphDatabase.driver(url, auth=auth) as driver:
        while time.perf_counter() - t0 < TIMEOUT:
            if check_availability(driver):
                break
            time.sleep(0.1)
        else:
            print("Last error:", LAST_ERROR)
            raise TimeoutError(
                "Timed out waiting for databases to become available at "
                f"{host}:{port}"
            )


if __name__ == "__main__":
    main(*sys.argv[1:5])
