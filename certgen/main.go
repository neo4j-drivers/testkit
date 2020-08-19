package main

// Utility to generate certificates for tests.
// Built and executed on  developer host machine, certificates and keys are checked into
// version control.
// No need to build this when running tests.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path"
	"time"
)

func createFile(path string) *os.File {
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	return file
}

func writeKey(path string, keyx interface{}) {
	file := createFile(path)
	defer file.Close()

	switch key := keyx.(type) {
	case *ecdsa.PrivateKey:
		m, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			panic(err)
		}
		err = pem.Encode(file, &pem.Block{Type: "EC PRIVATE KEY", Bytes: m})
		if err != nil {
			panic(err)
		}
	default:
		panic("Unknown key type")
	}
}

func writeCert(path string, der []byte) {
	file := createFile(path)
	defer file.Close()

	err := pem.Encode(file, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err != nil {
		panic(err)
	}
}

func newSerialNumber() *big.Int {
	max := (&big.Int{}).Lsh(big.NewInt(1), 128)
	ser, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	return ser
}

func newEcdsaKey() *ecdsa.PrivateKey {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return key
}

func generateRoot(notBefore, notAfter time.Time, commonName string) (*x509.Certificate, interface{}, []byte) {
	key := newEcdsaKey()
	template := x509.Certificate{
		SerialNumber:          newSerialNumber(),
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		Subject:               pkix.Name{CommonName: commonName},
		KeyUsage:              x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return &template, key, derBytes
}

func generateServer(parent *x509.Certificate, parentPrivate interface{}, notBefore, notAfter time.Time, commonName, dnsName string) (interface{}, []byte) {
	key := newEcdsaKey()
	template := x509.Certificate{
		SerialNumber:          newSerialNumber(),
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		Subject:               pkix.Name{CommonName: commonName},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              []string{dnsName},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent, &key.PublicKey, parentPrivate)
	if err != nil {
		panic(err)
	}
	return key, derBytes
}

func main() {
	basePath := path.Join(os.Args[1], "certs")

	now := time.Now()
	tenYearsFromNow := now.Add(time.Hour * 24 * 365 * 20)
	anHourAgo := now.Add(time.Hour * -1)

	// trustedRoot
	// Trusted by drivers. trustedRoot.pem should be installed on driver Docker image among trusted
	// root CAs.
	trustedRootCert, trustedRootKey, trustedRootDer := generateRoot(anHourAgo, tenYearsFromNow, "trustedRoot")
	writeKey(path.Join(basePath, "trustedRoot.key"), trustedRootKey)
	// CRT files contains multiple certificates, as long as we only have one trusted in the driver it's
	// fine to just write it as this.
	// The name ca-certificates.crt assumes that driver Docker images are based on Debian and path
	// should be mounted as /etc/ssl/certs/
	writeCert(path.Join(basePath, "driver", "trustedRoot.crt"), trustedRootDer)

	// trustedRoot_server1
	// Valid dates with hostname set to something that drivers can connect to from driver
	// Docker container.
	server1Key, server1Der := generateServer(trustedRootCert, trustedRootKey, anHourAgo, tenYearsFromNow, "trustedRoot_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "trustedRoot_thehost.key"), server1Key)
	writeCert(path.Join(basePath, "server", "trustedRoot_thehost.pem"), server1Der)

	// trustedRoot_server2
	// Invalid dates, otherwise same as server1.

	// untrustedRoot
	// Not trusted by drivers otherwise same as trustedRoot.

	// untrustedRoot_server1
	// Different root, otherwise same as trustedRoot_server1

	// untrustedRoot_server2
	// Different root, otherwise same as trustedRoot_server2
}
