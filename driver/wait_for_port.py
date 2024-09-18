import socket
import sys
import time


def wait_for_port(address, port):
    start = time.perf_counter()
    timeout = 120
    while True:
        try:
            with socket.create_connection((address, port), timeout):
                return True
        except OSError:
            time.sleep(0.1)
            if time.perf_counter() - start > timeout:
                break
    print(f"ERROR: Timeout while waiting for port {port} on {address}",
          file=sys.stderr, flush=True)
    return False


if __name__ == "__main__":
    if not wait_for_port(sys.argv[1], sys.argv[2]):
        sys.exit(-1)
