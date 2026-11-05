package dataplane_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/testutil"
)

type (
	// credentialRecorder is a [creds.Reporter] that keeps the credential label of
	// every expiry it is told about, once per report.
	credentialRecorder struct {
		mu    sync.Mutex
		names []string
	}

	// servingCert is one certificate the gateway can present, with the PEM it is
	// written to disk from.
	servingCert struct {
		serial  *big.Int
		cert    *x509.Certificate
		certPEM []byte
		keyPEM  []byte
	}
)

func TestNew_RegistersEveryTLSCredential(t *testing.T) {
	t.Parallel()

	gatewayCert, gatewayKey := testutil.GenerateSelfSignedCert(t)
	primaryCA, _, _ := testutil.GenerateMTLSCerts(t)
	tmplCA, _, _ := testutil.GenerateMTLSCerts(t)
	cloudAPICA, _, _ := testutil.GenerateMTLSCerts(t)

	cfg := testConfig()
	cfg.Listen.TLS = &config.TLSConfig{Cert: gatewayCert, Key: gatewayKey}
	cfg.Upstreams = config.UpstreamList{
		{
			Name:   "primary",
			Listen: config.ListenConfig{HostPort: "127.0.0.1:7233", TLS: &config.TLSConfig{CA: primaryCA}},
		},
		{
			Name:   "tmpl",
			Listen: config.ListenConfig{HostPort: "{{ .LocalNamespace }}.tmpl.example:7233", TLS: &config.TLSConfig{CA: tmplCA}},
		},
		// Both Cloud frontends verify against the system roots, so they register
		// nothing, and both share the one Cloud API block.
		{Name: "cloud-a", Cloud: true, Listen: config.ListenConfig{HostPort: "ns.a.tmprl.cloud:7233"}},
		{Name: "cloud-b", Cloud: true, Listen: config.ListenConfig{HostPort: "ns.b.tmprl.cloud:7233"}},
	}
	cfg.Routing.Rules = []config.RoutingRule{
		{Upstream: "tmpl", Match: config.RoutingMatch{Namespace: "*.tmpl"}},
		{Upstream: "cloud-a", Match: config.RoutingMatch{Namespace: "*.a"}},
		{Upstream: "cloud-b", Match: config.RoutingMatch{Namespace: "*.b"}},
	}
	cfg.APITranslations = stagingCloudAPI()
	cfg.APITranslations.CloudAPI.Listen.TLS = &config.TLSConfig{CA: cloudAPICA}

	rec := &credentialRecorder{}
	r := creds.NewReloader(creds.WithReporter(rec))

	deps := newTestDeps(t, cfg)
	_, err := dataplane.New(deps.ctx, cfg, append(deps.opts(), dataplane.WithReloader(r))...)
	require.NoError(t, err)

	startReloader(t, r)

	// One report per file set: each of these presents a cert or trusts a CA, not
	// both, and the two Cloud upstreams share one Cloud API file set.
	require.ElementsMatch(t,
		[]string{"cloud_api", "gateway", "upstream/primary", "upstream/tmpl"},
		rec.credentials(),
	)
}

func TestGateway_PicksUpRotatedCertificate(t *testing.T) {
	t.Parallel()

	first, second := newServingCert(t, 100), newServingCert(t, 200)

	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	first.writeTo(t, certFile, keyFile)

	cfg := liveConfig(t)
	cfg.Listen.TLS = &config.TLSConfig{Cert: certFile, Key: keyFile}

	r := creds.NewReloader(creds.WithReloadInterval(50 * time.Millisecond))

	deps := newTestDeps(t, cfg)
	dp, err := dataplane.New(deps.ctx, cfg, append(deps.opts(), dataplane.WithReloader(r))...)
	require.NoError(t, err)

	startReloader(t, r)

	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, dp.Start(startCtx))
	t.Cleanup(func() { _ = dp.Stop(context.WithoutCancel(t.Context())) })

	roots := x509.NewCertPool()
	roots.AddCert(first.cert)
	roots.AddCert(second.cert)
	tlsCfg := &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}

	addr := dp.Addr().String()
	require.Zero(t, first.serial.Cmp(leafSerial(addr, tlsCfg)))

	// Handshake before the rotation, so this connection holds the old material.
	// It trusts only the first certificate, so a reconnect after the rotation
	// would fail verification: the Check after it passes only if this
	// connection survived.
	oldRoots := x509.NewCertPool()
	oldRoots.AddCert(first.cert)
	oldCfg := &tls.Config{RootCAs: oldRoots, ServerName: "localhost", MinVersion: tls.VersionTLS12}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(oldCfg)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	health := grpc_health_v1.NewHealthClient(conn)
	checkCtx, cancelCheck := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelCheck()
	_, err = health.Check(checkCtx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	second.writeTo(t, certFile, keyFile)

	require.Eventually(t, func() bool {
		return second.serial.Cmp(leafSerial(addr, tlsCfg)) == 0
	}, 5*time.Second, 50*time.Millisecond, "new handshakes must present the rotated certificate")

	afterCtx, cancelAfter := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelAfter()
	_, err = health.Check(afterCtx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err, "a connection opened before the rotation must keep working")
}

func (c *credentialRecorder) Attempt(string, error) {}

func (c *credentialRecorder) NotAfter(credential, _ string, _ time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.names = append(c.names, credential)
}

func (c *credentialRecorder) credentials() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.names)
}

// writeTo replaces the certificate and key files, each by rename, so the
// reloader never reads a partially written file.
func (s servingCert) writeTo(t *testing.T, certFile, keyFile string) {
	t.Helper()

	for path, data := range map[string][]byte{certFile: s.certPEM, keyFile: s.keyPEM} {
		tmp := path + ".tmp"
		require.NoError(t, os.WriteFile(tmp, data, 0o600))
		require.NoError(t, os.Rename(tmp, path))
	}
}

// newServingCert mints a self-signed "localhost" server certificate with the
// given serial number.
func newServingCert(t *testing.T, serial int64) servingCert {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return servingCert{
		serial:  tmpl.SerialNumber,
		cert:    cert,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// leafSerial opens a fresh TLS connection to addr and returns the serial number
// of the certificate the server presented, or -1 when the handshake fails. It
// does not fail the test, so it is safe to call from a require.Eventually
// condition.
func leafSerial(addr string, cfg *tls.Config) *big.Int {
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return big.NewInt(-1)
	}

	defer func() { _ = conn.Close() }()

	return conn.ConnectionState().PeerCertificates[0].SerialNumber
}

// startReloader starts r, after every credential has registered with it, and
// stops it when the test ends.
func startReloader(t *testing.T, r *creds.Reloader) {
	t.Helper()

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, r.Stop(context.WithoutCancel(t.Context()))) })
}
