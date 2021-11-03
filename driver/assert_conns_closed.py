# Checks if there are any connections open to the specified remote address+port
import socket
import subprocess
import sys

if __name__ == "__main__":
    address = sys.argv[1]
    port = sys.argv[2]

    portx = format(int(port), '04X')
    host = socket.gethostbyname(address)
    addressx = format(socket.ntohl(int.from_bytes(socket.inet_aton(host),
                                                  byteorder='big',
                                                  signed=False)), '08X')
    remote = "%s:%s" % (addressx, portx)

    conns = subprocess.check_output(
            ["cat", "/proc/net/tcp"], universal_newlines=True)
    conns = conns.splitlines()[1:]  # Skip header

    # COLUMNS (of interest)
    # 0  Entry number
    # 1  Local IPv4 address and port
    # 2  Remote IPv4 address and port
    # 3  Connection state
    active = []
    for conn in conns:
        conn = conn.split()
        # First check if the remote address matches
        if conn[2] == remote:
            state = conn[3]
            # TCP_ESTABLISHED (01), TCP_SYN_SENT (02) and
            # TCP_SYN_RECV (03) are not legal
            if state in ['01', '02', '03']:
                active.append(conn)

    if len(active) > 0:
        print("ERROR: Connections to %s:%s are still open: %s" % (address, port, active))
        sys.exit(-1)
    sys.exit(0)
