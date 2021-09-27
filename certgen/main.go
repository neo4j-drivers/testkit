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
	tenYearsAgo := now.Add(-1 * time.Hour * 24 * 365 * 20)
	anHourAgo := now.Add(time.Hour * -1)

	// trustedRoot
	// Trusted by drivers. trustedRoot.pem should be installed in driver Docker image among trusted
	// root CAs.
	trustedRootCert, trustedRootKey, trustedRootDer := generateRoot(anHourAgo, tenYearsFromNow, "trustedRoot")
	writeKey(path.Join(basePath, "trustedRoot.key"), trustedRootKey)
	// customRoot + customRoot2
	// Not trusted by the drivers by default. customRoot.crt should be installed in driver Docker image
	// in some place. Testkit will only refer to it with it's name (no path). It will be used to test custom CA driver settings.
	customRootCert, customRootKey, customRootDer := generateRoot(anHourAgo, tenYearsFromNow, "customRoot")
	writeKey(path.Join(basePath, "customRoot.key"), customRootKey)
	customRoot2Cert, customRoot2Key, customRoot2Der := generateRoot(anHourAgo, tenYearsFromNow, "customRoot2")
	writeKey(path.Join(basePath, "customRoot2.key"), customRoot2Key)
	// CRT files contains multiple certificates, as long as we only have one trusted in the driver it's
	// fine to just write it as this.
	// The name ca-certificates.crt assumes that driver Docker images are based on Debian and path
	// should be mounted as /etc/ssl/certs/
	writeCert(path.Join(basePath, "driver", "trusted", "trustedRoot.crt"), trustedRootDer)
	writeCert(path.Join(basePath, "driver", "custom", "customRoot.crt"), customRootDer)
	writeCert(path.Join(basePath, "driver", "custom", "customRoot2.crt"), customRoot2Der)

	// trustedRoot_server1
	// Valid dates with hostname set to something that drivers can connect to from driver // Docker container.
	trustedRoot_server1Key, trustedRoot_server1Der := generateServer(trustedRootCert, trustedRootKey, anHourAgo, tenYearsFromNow, "trustedRoot_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "trustedRoot_thehost.key"), trustedRoot_server1Key)
	writeCert(path.Join(basePath, "server", "trustedRoot_thehost.pem"), trustedRoot_server1Der)
	// customRoot_server1
	// now repeat the whole thing for the custom CAs
	customRoot_server1Key, customRoot_server1Der := generateServer(customRootCert, customRootKey, anHourAgo, tenYearsFromNow, "customRoot_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "customRoot_thehost.key"), customRoot_server1Key)
	writeCert(path.Join(basePath, "server", "customRoot_thehost.pem"), customRoot_server1Der)
	customRoot2_server1Key, customRoot2_server1Der := generateServer(customRoot2Cert, customRoot2Key, anHourAgo, tenYearsFromNow, "customRoot2_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "customRoot2_thehost.key"), customRoot2_server1Key)
	writeCert(path.Join(basePath, "server", "customRoot2_thehost.pem"), customRoot2_server1Der)

	// trustedRoot_server2
	// Expired dates, otherwise same as server1.
	trustedRoot_server2Key, trustedRoot_server2Der := generateServer(trustedRootCert, trustedRootKey, tenYearsAgo, anHourAgo, "trustedRoot_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "trustedRoot_thehost_expired.key"), trustedRoot_server2Key)
	writeCert(path.Join(basePath, "server", "trustedRoot_thehost_expired.pem"), trustedRoot_server2Der)
	// customRoot_server2
	// now repeat the whole thing for the custom CA
	customRoot_server2Key, customRoot_server2Der := generateServer(customRootCert, customRootKey, tenYearsAgo, anHourAgo, "customRoot_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "customRoot_thehost_expired.key"), customRoot_server2Key)
	writeCert(path.Join(basePath, "server", "customRoot_thehost_expired.pem"), customRoot_server2Der)

	// untrustedRoot
	// Not trusted by drivers otherwise same as trustedRoot.
	untrustedRootCert, untrustedRootKey, _ := generateRoot(anHourAgo, tenYearsFromNow, "untrustedRoot")
	writeKey(path.Join(basePath, "trustedRoot.key"), untrustedRootKey)
	// Do not write the DER to driver/*.crt folder, that would install it as trusted!

	// untrustedRoot_server1
	// Different root, otherwise same as trustedRoot_server1
	untrustedRoot_server1Key, untrustedRoot_server1Der := generateServer(untrustedRootCert, untrustedRootKey, anHourAgo, tenYearsFromNow, "untrustedRoot_thehost", "thehost")
	writeKey(path.Join(basePath, "server", "untrustedRoot_thehost.key"), untrustedRoot_server1Key)
	writeCert(path.Join(basePath, "server", "untrustedRoot_thehost.pem"), untrustedRoot_server1Der)
}
