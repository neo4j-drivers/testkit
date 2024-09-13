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


def check_availability(driver, address):
    global LAST_ERROR
    address = f"{address}"
    try:
        records, _, _ = driver.execute_query(
            "SHOW DATABASES "
            "YIELD name, address, requestedStatus, currentStatus",
            database_="system",
        )
        db_names = set()
        print("Records:", records)
        for record in records:
            name = record.get("name")
            if not isinstance(name, str):
                LAST_ERROR = "name not str"
                db_names.add(name)
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
            rec_address = record.get("address")
            if not isinstance(rec_address, str):
                LAST_ERROR = "address not str"
            if rec_address != address:
                continue  # db on different server
            name = record.get("name")
            if not isinstance(name, str):
                LAST_ERROR = "name not str"
            db_names.add(name)
        if not {"system", "neo4j"} <= db_names:
            LAST_ERROR = (
                "not {'system', 'neo4j'} <= db_names: "
                f"{db_names!r}"
            )
            return False
        return True
    except (DriverError, Neo4jError) as e:
        LAST_ERROR = str(e)
        return False


def main(address, user, password):
    url = f"bolt://{address}"
    auth = (user, password)
    t0 = time.perf_counter()

    with neo4j.GraphDatabase.driver(url, auth=auth) as driver:
        while time.perf_counter() - t0 < TIMEOUT:
            print("Checking availability...")
            if check_availability(driver, address):
                break
            time.sleep(.5)
        else:
            print("Last error:", LAST_ERROR, file=sys.stderr, flush=True)
            raise TimeoutError(
                "Timed out waiting for databases to become available at "
                f"{address}"
            )


if __name__ == "__main__":
    main(*sys.argv[1:4])
