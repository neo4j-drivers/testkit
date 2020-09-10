import sys, time, socket

def wait_for_port(address, port):
    start = time.perf_counter()
    timeout = 30
    while True:
        try:
            with socket.create_connection((address, port), timeout):
                return True
        except OSError or ConnectionRefusedError:
            time.sleep(0.1)
            if time.perf_counter() - start > timeout:
                print("ERROR: Timeout while waiting for port %s on %s" % (port, address))
                return False

if __name__ == "__main__":
    if not wait_for_port(sys.argv[1], sys.argv[2]):
        sys.exit(-1)
