package hub

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bubustack/core/contracts"
)

func TestHubTLSConfigFromEnvRequiresCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, _ := writeHubTLSMaterial(t, dir)

	t.Setenv(contracts.HubTLSCertFileEnv, certFile)
	t.Setenv(contracts.HubTLSKeyFileEnv, keyFile)
	t.Setenv(contracts.HubCAFileEnv, filepath.Join(dir, "missing-ca.crt"))

	if _, err := hubTLSConfigFromEnv(); err == nil {
		t.Fatalf("expected missing hub CA to be rejected")
	}
}

func TestHubTLSConfigFromEnvEnforcesTLS13AndClientAuth(t *testing.T) {
	certFile, keyFile, caFile := writeHubTLSMaterial(t, t.TempDir())

	t.Setenv(contracts.HubTLSCertFileEnv, certFile)
	t.Setenv(contracts.HubTLSKeyFileEnv, keyFile)
	t.Setenv(contracts.HubCAFileEnv, caFile)

	tlsConfig, err := hubTLSConfigFromEnv()
	if err != nil {
		t.Fatalf("hubTLSConfigFromEnv failed: %v", err)
	}
	if tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3 minimum, got %d", tlsConfig.MinVersion)
	}
	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected client certificate verification, got %v", tlsConfig.ClientAuth)
	}
	if tlsConfig.ClientCAs == nil {
		t.Fatalf("expected hub client CA pool to be configured")
	}
}

func writeHubTLSMaterial(t *testing.T, dir string) (string, string, string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "bobravoz-grpc-hub.default.svc",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	certFile := filepath.Join(dir, "hub.crt")
	keyFile := filepath.Join(dir, "hub.key")
	caFile := filepath.Join(dir, "ca.crt")

	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	return certFile, keyFile, caFile
}
