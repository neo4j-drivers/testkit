package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
)

func exitWithError(err error) {
	fmt.Println(err)
	os.Exit(-1)
}

var expectedHandshake = []byte{0x60, 0x60, 0xb0, 0x17}

// Do not print anything on stdout until listening!
func main() {
	// Address that server binds to
	address := os.Args[1]
	// Server certificate and private key paths
	certPath := os.Args[2]
	keyPath := os.Args[3]
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		exitWithError(err)
	}

	config := tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			// TODO: Set cert.OCSPStaple
			return &cert, nil
		},
	}
	listener, err := tls.Listen("tcp", address, &config)
	if err != nil {
		exitWithError(err)
	}
	fmt.Printf("TLS, listening on %s with cert %s\n", address, certPath)
	defer listener.Close()

	conn, err := listener.Accept()
	if err != nil {
		exitWithError(err)
	}
	fmt.Printf("TLS, client connected from %s, waiting for Bolt handshake\n", conn.RemoteAddr())

	handshake := make([]byte, 4*5)
	_, err = io.ReadFull(conn, handshake)
	if err != nil {
		fmt.Println("Failed to receive Bolt handshake")
		exitWithError(err)
	}
	conn.Close()

	// Just check the signature
	for i, x := range expectedHandshake {
		if x != handshake[i] {
			exitWithError(errors.New("Bad Bolt handshake"))
		}
	}

	fmt.Println("Client connected with correct Bolt handshake")
}
