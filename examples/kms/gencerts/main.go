// Command gencerts writes a throwaway CA and a server certificate for the
// example's extension server into certs/.
//
// The CA is private to this example: it is never installed in a system trust
// store, and the proxy is pointed at it explicitly through the tls.ca field in
// config.yaml. Its private key is discarded once the server certificate is
// signed, so nothing else can ever be signed by it.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// validFor is generous so a checkout left alone for a while does not fail with a
// confusing expiry error.
const validFor = 365 * 24 * time.Hour

func main() {
	dir := flag.String("dir", "certs", "directory to write ca.pem, server.pem and server-key.pem into")
	flag.Parse()

	if err := run(*dir); err != nil {
		log.Fatalf("gencerts: %v", err)
	}
}

func run(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "temporal-proxy kms example CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// DNSNames carries "localhost" and no IP address on purpose. config.yaml
	// dials 127.0.0.1, so verification only succeeds because it also sets
	// tls.serverName to localhost. Adding an IP SAN here would make that setting
	// look optional.
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(validFor),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return err
	}

	files := []struct {
		name  string
		block *pem.Block
		perm  os.FileMode
	}{
		{name: "ca.pem", block: &pem.Block{Type: "CERTIFICATE", Bytes: caDER}, perm: 0o644},
		{name: "server.pem", block: &pem.Block{Type: "CERTIFICATE", Bytes: leafDER}, perm: 0o644},
		{name: "server-key.pem", block: &pem.Block{Type: "PRIVATE KEY", Bytes: leafKeyDER}, perm: 0o600},
	}

	for _, f := range files {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, pem.EncodeToMemory(f.block), f.perm); err != nil {
			return err
		}

		log.Printf("wrote %s", path)
	}

	return nil
}
