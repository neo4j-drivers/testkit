import sys
import time

import neo4j
from neo4j.debug import watch
from neo4j.exceptions import (
    DriverError,
    Neo4jError,
)

watch("neo4j", out=sys.stdout)


TIMEOUT = 120
LAST_ERROR = ""


def check_availability(driver):
    global LAST_ERROR
    try:
        records, _, _ = driver.execute_query(
            "SHOW DATABASES YIELD name, requestedStatus, currentStatus",
            database_="system",
        )
        if not records:
            LAST_ERROR = "No records returned"
            return False
        print("Records:", records)
        for record in records:
            status_req = record.get("requestedStatus")
            if not isinstance(status_req, str):
                LAST_ERROR = "requestedStatus not str"
                return False
            status_cur = record.get("currentStatus")
            if not isinstance(status_cur, str):
                LAST_ERROR = "currentStatus not str"
                return False
            if not status_req == status_cur == "online":
                LAST_ERROR = (
                    'not status_req == status_cur == "online": '
                    f'{status_req!r} == {status_cur!r} == "online"'
                )
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
            print("Checking availability...")
            if check_availability(driver):
                break
            time.sleep(.5)
        else:
            print("Last error:", LAST_ERROR)
            raise TimeoutError(
                "Timed out waiting for databases to become available at "
                f"{host}:{port}"
            )


if __name__ == "__main__":
    main(*sys.argv[1:5])
