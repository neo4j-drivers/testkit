import ssl, socket, os, sys

"""
Process exit code will be 0 if a client succesfully connects and sends a proper
handshake.

All other types of errors will raise and exit with non zero.
"""
if __name__ == "__main__":
    # Retrieve path to the repository containing this script.
    serverCertPath = sys.argv[1]
    serverKeyPath = sys.argv[2]
    print("Starting TLS server with cert %s and key %s" % (serverCertPath, serverKeyPath))

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(serverCertPath, serverKeyPath)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', 6666))
        sock.listen(5)
        with context.wrap_socket(sock, server_side=True) as ssock:
            while True:
                try:
                    conn, addr = ssock.accept()
                    print("Connected to %s. Receiving handshake.." % addr[0])
                    n = 4*5
                    handshake = []
                    while len(handshake) != n:
                        handshake.extend(conn.recv(n-len(handshake)))
                    conn.close()
                    if handshake[:4] != [0x60, 0x60, 0xb0, 0x17]:
                        raise Exception("Bad handshake")
                    print("Got proper handshake")
                    sys.exit(0)
                except ssl.SSLError as e:
                    print(e)
