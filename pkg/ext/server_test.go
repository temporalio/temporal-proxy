package ext_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

const (
	// The credential these tests present to the extension server itself, as opposed
	// to anything a caller of the proxy presents.
	extHeader = "x-ext-token"
	extToken  = "s3cret"

	// bufSize is the in-memory listener's buffer, large enough that the oversized
	// request in TestReceiveLimit is refused by the server's own limit rather than
	// wedged in the pipe.
	bufSize = 4 * 1024 * 1024

	// plaintextMessage is the warning ext logs for an unencrypted connection.
	// Spelled out here rather than exported: publishing a log line as API would
	// promise not to reword it.
	plaintextMessage = "Serving in plaintext. Supply credentials via WithServerOption for production use."
)

// blockingKMS enters Wrap, records that it did, and stays there until released.
// It is how these tests hold a call in flight across a shutdown.
type blockingKMS struct {
	release <-chan struct{}
	log     *logger.TestLogger
}

func TestServeRegistersBothServices(t *testing.T) {
	t.Parallel()

	// Neither WithAuth nor WithKMS: the services still answer, which is the
	// difference between a proxy learning it asked for something this server does
	// not do and a proxy failing to connect at all.
	cc := dial(t, serve(t), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), &kms.EncryptRequest{
		Namespace: "ns",
		Plaintext: []byte("dek"),
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = kms.NewEncryptionServiceClient(cc).Decrypt(t.Context(), &kms.DecryptRequest{
		Ciphertext: []byte("ct"),
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestServeShutsDownOnContextCancel(t *testing.T) {
	t.Parallel()

	// Over a real port rather than bufconn, so the listener Serve opens for itself
	// is covered on the success path and not only by TestServeReturnsListenError.
	addr := reserveAddr(t)
	log := logger.NewTestLogger()
	ctx, cancel := context.WithCancel(t.Context())

	errs := make(chan error, 1)
	go func() {
		errs <- ext.Serve(ctx, ext.WithAddr(addr), ext.WithLogger(log))
	}()

	// Serving before the cancel, so what follows is a shutdown rather than a
	// server that never started.
	cc := dialTarget(t, addr, insecure.NewCredentials())
	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	cancel()
	require.NoError(t, waitFor(t, errs), "a shutdown is not an error")

	require.True(t, log.Contains("Shutdown signal received"))
	require.True(t, log.Contains("Server stopped cleanly"))
	require.False(t, log.Contains("GracefulStop timed out. Forcing shutdown"))
}

func TestServeReturnsListenError(t *testing.T) {
	t.Parallel()

	// Held open, so Serve's own bind fails.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })

	log := logger.NewTestLogger()
	err = ext.Serve(t.Context(), ext.WithAddr(held.Addr().String()), ext.WithLogger(log))

	// Returned rather than logged and swallowed: a server that cannot bind has to
	// fail the process, since the proxy would otherwise be pointed at nothing.
	require.ErrorIs(t, err, syscall.EADDRINUSE, "a bind conflict should surface as one")
	require.False(t, log.Contains("Shutdown signal received"), "never started, so never shut down")
}

func TestServeRejectsServerAuthWithoutHeader(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()

	// Off the main goroutine only so a regression that starts the server anyway
	// fails this test rather than hanging the package.
	errs := make(chan error, 1)
	go func() {
		errs <- ext.Serve(t.Context(),
			ext.WithListener(bufconn.Listen(bufSize)),
			ext.WithLogger(log),
			ext.WithServerAuth("", acceptExtToken),
		)
	}()

	// A guard on an unnamed header rejects every call as missing credentials. The
	// proxy fails closed on that, so it presents as an outage somewhere else
	// entirely; refusing to start names the actual fault instead.
	require.EqualError(t, waitFor(t, errs), "a header name is required to guard this server")
	require.False(t, log.Contains("Starting extension server"), "refused before it served")
}

func TestWithListenerIgnoresAddr(t *testing.T) {
	t.Parallel()

	// Reserved but never handed to Serve, because WithListener wins.
	addr := reserveAddr(t)
	cc := dial(t, serve(t, ext.WithAddr(addr)), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "served on the supplied listener")

	// Still bindable, which is what proves Serve left it alone rather than quietly
	// listening in two places.
	held, err := net.Listen("tcp", addr)
	require.NoError(t, err, "WithAddr should be ignored when a listener is supplied")
	require.NoError(t, held.Close())
}

func TestWithLoggerIgnoresNil(t *testing.T) {
	t.Parallel()

	// A nil logger is the last option applied here, so it would overwrite the
	// harness default and panic on the first line the server logged. It is dropped
	// instead, which matters for a caller whose logger came from a config branch
	// that happened not to fire.
	cc := dial(t, serve(t, ext.WithLogger(nil)), nil)

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestServeForcesShutdownWhenHandlerHangs(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	log := logger.NewTestLogger()
	lis := bufconn.Listen(bufSize)
	ctx, cancel := context.WithCancel(t.Context())

	errs := make(chan error, 1)
	go func() {
		errs <- ext.Serve(ctx,
			ext.WithListener(lis),
			ext.WithLogger(log),
			ext.WithKMS(&blockingKMS{release: release, log: log}),
			ext.WithShutdownTimeout(100*time.Millisecond),
		)
	}()

	// Wedged in the handler for the rest of the test, so GracefulStop has an
	// in-flight call it will wait on indefinitely. Dialed here rather than in the
	// goroutine: dial registers cleanups and asserts, neither of which belongs off
	// the test's own goroutine.
	client := kms.NewEncryptionServiceClient(dial(t, lis, nil))
	hung := make(chan error, 1)
	go func() {
		_, err := client.Encrypt(
			context.WithoutCancel(t.Context()),
			&kms.EncryptRequest{Namespace: "ns", Plaintext: []byte("dek")},
		)
		hung <- err
	}()

	requireEventually(t, func() bool { return log.Contains("wrapping") }, "handler never entered")

	cancel()
	require.NoError(t, waitFor(t, errs), "a forced shutdown is still a shutdown")

	require.True(t, log.Contains("GracefulStop timed out. Forcing shutdown"))
	require.False(t, log.Contains("Server stopped cleanly"))

	// Stop closes the transports before it can lose the lock race with the
	// GracefulStop it is abandoning, so the wedged call is dropped either way.
	require.Error(t, waitFor(t, hung))
}

func TestWithShutdownTimeoutClampsToFloor(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	log := logger.NewTestLogger()
	lis := bufconn.Listen(bufSize)
	ctx, cancel := context.WithCancel(t.Context())

	errs := make(chan error, 1)
	go func() {
		// Zero would otherwise fire the timeout the instant shutdown began,
		// discarding a call that had already been answered but not yet flushed.
		errs <- ext.Serve(ctx,
			ext.WithListener(lis),
			ext.WithLogger(log),
			ext.WithKMS(&blockingKMS{release: release, log: log}),
			ext.WithShutdownTimeout(0),
		)
	}()

	client := kms.NewEncryptionServiceClient(dial(t, lis, nil))
	go func() {
		_, _ = client.Encrypt(
			context.WithoutCancel(t.Context()),
			&kms.EncryptRequest{Namespace: "ns", Plaintext: []byte("dek")},
		)
	}()

	requireEventually(t, func() bool { return log.Contains("wrapping") }, "handler never entered")

	// Real time rather than testing/synctest: the bubble's clock only advances once
	// every goroutine in it is durably blocked, and gRPC's transport goroutines sit
	// on real network reads, which never qualify.
	//
	// A lower bound, not an upper one. The handler never returns, so the only thing
	// that can end the wait is the clamped timeout expiring.
	start := time.Now()
	cancel()
	require.NoError(t, waitFor(t, errs))

	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	require.True(t, log.Contains("GracefulStop timed out. Forcing shutdown"))
}

func TestServeOverTLS(t *testing.T) {
	t.Parallel()

	lis, roots := serveTLS(t)

	// The certificate is issued for localhost and an in-memory listener has no name
	// of its own, so the name has to be supplied for verification to have anything
	// to match against.
	cc := dial(t, lis, credentials.NewTLS(&tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}))

	_, err := auth.NewAuthServiceClient(cc).Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "reached the handler over TLS")
}

func TestServeOverTLSRejectsPlaintextDialer(t *testing.T) {
	t.Parallel()

	lis, _ := serveTLS(t)

	// The mismatch WithServerOption warns about. WaitForReady would turn this into
	// a hang rather than an error, so this conn is built without it and bounded by
	// a deadline instead.
	cc, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(
			func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) },
		),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err = auth.NewAuthServiceClient(cc).Auth(ctx, &auth.AuthRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestReceiveLimit(t *testing.T) {
	t.Parallel()

	// Over the 1MiB default, which is sized for key material. A DEK is a few dozen
	// bytes, so anything near this is not one.
	oversized := &kms.EncryptRequest{Namespace: "ns", Plaintext: make([]byte, 2*1024*1024)}

	refused := &stubKMS{ciphertext: []byte("wrapped")}
	cc := dial(t, serve(t, ext.WithKMS(refused)), nil)

	_, err := kms.NewEncryptionServiceClient(cc).Encrypt(t.Context(), oversized)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.False(t, refused.wrapCalled(), "refused by the transport, so the implementation never saw it")

	// Raised through a server option, which is applied after the default and so
	// replaces it.
	accepted := &stubKMS{ciphertext: []byte("wrapped")}
	raised := dial(t, serve(t,
		ext.WithKMS(accepted),
		ext.WithServerOption(grpc.MaxRecvMsgSize(4*1024*1024)),
	), nil)

	_, err = kms.NewEncryptionServiceClient(raised).Encrypt(t.Context(), oversized)
	require.NoError(t, err)
	require.True(t, accepted.wrapCalled())
}

func TestChainedInterceptorRunsAfterTheGuard(t *testing.T) {
	t.Parallel()

	seen := make(chan string, 1)
	client := auth.NewAuthServiceClient(dial(t, serve(t,
		ext.WithAuth(&stubAuth{}),
		ext.WithServerOption(grpc.ChainUnaryInterceptor(recordMethod(seen))),
		ext.WithServerAuth(extHeader, acceptExtToken),
	), nil))

	// No credential, so the guard rejects the call before the chain is reached.
	_, err := client.Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Empty(t, seen, "the guard is chained ahead of anything added by a caller")

	// With the credential, the same interceptor runs, so what it does see it may
	// treat as already vetted.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(extHeader, extToken))
	_, err = client.Auth(ctx, &auth.AuthRequest{})
	require.NoError(t, err)
	require.Equal(t, "/api.auth.v1.AuthService/Auth", <-seen)
}

func TestUnaryInterceptorRunsBeforeTheGuard(t *testing.T) {
	t.Parallel()

	seen := make(chan string, 1)
	client := auth.NewAuthServiceClient(dial(t, serve(t,
		ext.WithAuth(&stubAuth{}),
		// The one ordering this package does not get to decide: gRPC prepends
		// grpc.UnaryInterceptor ahead of the whole chain, guard included.
		ext.WithServerOption(grpc.UnaryInterceptor(recordMethod(seen))),
		ext.WithServerAuth(extHeader, acceptExtToken),
	), nil))

	_, err := client.Auth(t.Context(), &auth.AuthRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// It saw the call the guard turned away, which is why this one is for counting
	// rejections rather than for anything that assumes a vetted caller.
	require.Equal(t, "/api.auth.v1.AuthService/Auth", <-seen)
}

func (b *blockingKMS) Wrap(_ context.Context, _ string, _ []byte) ([]byte, error) {
	b.log.Info("wrapping")
	<-b.release

	return nil, status.Error(codes.Unavailable, "never answered")
}

func (b *blockingKMS) Unwrap(context.Context, []byte) ([]byte, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func TestServeRegistersHealthService(t *testing.T) {
	t.Parallel()

	client := grpc_health_v1.NewHealthClient(dial(t, serve(t), nil))

	resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestHealthIsExemptFromServerAuth(t *testing.T) {
	t.Parallel()

	conn := dial(t, serve(t, ext.WithServerAuth(extHeader, acceptExtToken)), nil)

	// No credential, because a probe has none to give.
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		t.Context(),
		&grpc_health_v1.HealthCheckRequest{},
	)
	require.NoError(t, err, "guarding health would break every probe")
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	// The same call without a credential to a guarded service, to show the
	// exemption is confined to health rather than the guard being absent.
	_, err = auth.NewAuthServiceClient(conn).Auth(t.Context(), &auth.AuthRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestHealthReportsNotServingOnShutdown(t *testing.T) {
	t.Parallel()

	lis := bufconn.Listen(bufSize)
	ctx, cancel := context.WithCancel(t.Context())

	errs := make(chan error, 1)
	go func() {
		errs <- ext.Serve(ctx,
			ext.WithListener(lis),
			ext.WithLogger(logger.NewNoopLogger()),
			// A watcher holds the drain open, so this shutdown is forced by design.
			ext.WithShutdownTimeout(100*time.Millisecond),
		)
	}()

	stream, err := grpc_health_v1.NewHealthClient(dial(t, lis, nil)).Watch(
		t.Context(),
		&grpc_health_v1.HealthCheckRequest{},
	)
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, first.GetStatus())

	cancel()

	// Watchers are told before the door closes, so whatever routes here can stop
	// sending rather than discovering it through a refused connection.
	next, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, next.GetStatus())

	require.NoError(t, waitFor(t, errs))
}

func TestServeWarnsWhenServingPlaintext(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	client := grpc_health_v1.NewHealthClient(dial(t, serve(t, ext.WithLogger(log)), nil))

	// The warning reads the connection, not the configuration, so it needs a call
	// to have arrived before there is anything to report.
	_, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	require.True(t, log.ContainsEntry(logger.LevelWarn, plaintextMessage))
}

func TestServeDoesNotWarnWhenServingTLS(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	lis, roots := serveTLS(t, ext.WithLogger(log))

	// ServerName because the certificate is issued for localhost and an in-memory
	// listener has no name to match it against. Omitting it fails the handshake,
	// which WaitForReady turns into a hang rather than an error.
	client := grpc_health_v1.NewHealthClient(dial(t, lis, credentials.NewTLS(&tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})))

	_, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	require.False(t, log.Contains(plaintextMessage), "TLS via WithServerOption should be recognised")
}

// acceptExtToken is the stand-in for a real [ext.CredentialCheck]. A production
// one compares in constant time; this one only has to be a function that agrees
// with extToken.
func acceptExtToken(got string) bool { return got == extToken }

// dial returns a client for lis, plaintext when creds is nil.
func dial(t *testing.T, lis *bufconn.Listener, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()

	if creds == nil {
		creds = insecure.NewCredentials()
	}

	return dialTarget(t, "passthrough:///bufconn", creds, grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) },
	))
}

// dialTarget builds a client for target with WaitForReady on every call, so a
// server that has not finished starting is waited for rather than reported as
// Unavailable. That is what lets these tests skip polling for readiness.
func dialTarget(
	t *testing.T, target string, creds credentials.TransportCredentials, extra ...grpc.DialOption,
) *grpc.ClientConn {
	t.Helper()

	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	}, extra...)

	cc, err := grpc.NewClient(target, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

// recordMethod returns an interceptor that reports the method it was invoked for
// and then proceeds. The send is non-blocking so an interceptor that runs more
// often than a test reads cannot wedge a handler.
func recordMethod(seen chan<- string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		select {
		case seen <- info.FullMethod:
		default:
		}

		return handler(ctx, req)
	}
}

// requireEventually blocks until cond holds, failing with msg if it never does.
// Used to observe a handler being entered, which no channel from the client side
// can report: the call is still in flight.
func requireEventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, 5*time.Millisecond, msg)
}

// reserveAddr returns a loopback address that was bindable a moment ago. Only the
// few tests about the listener [ext.Serve] opens for itself need this; everything
// else runs over a [bufconn.Listener] and never guesses at a port.
func reserveAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	return addr
}

// serve starts a server on an in-memory listener and returns it. Cleanup asserts
// the shutdown, so every test that starts a server this way also checks that
// cancelling its context stops it without an error. Tests that are about shutdown
// itself drive Serve directly.
//
// A [bufconn.Listener] rather than a port: WithListener means these tests never
// have to guess at a free one, so there is no window between reserving an address
// and Serve binding it.
func serve(t *testing.T, opts ...ext.Option) *bufconn.Listener {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	ctx, cancel := context.WithCancel(t.Context())

	// Defaults first so a test can override any of them.
	opts = append([]ext.Option{
		ext.WithListener(lis),
		ext.WithLogger(logger.NewNoopLogger()),
	}, opts...)

	errs := make(chan error, 1)
	go func() { errs <- ext.Serve(ctx, opts...) }()

	t.Cleanup(func() {
		cancel()
		require.NoError(t, waitFor(t, errs))
	})

	return lis
}

// serveTLS starts a server holding a self-signed certificate and returns its
// listener along with a pool that trusts it. TLS arrives as a server option and
// lands after Serve's own insecure default, which is what lets it win rather than
// having to be unset first.
func serveTLS(t *testing.T, opts ...ext.Option) (lis *bufconn.Listener, roots *x509.CertPool) {
	t.Helper()

	certFile, keyFile := testutil.GenerateSelfSignedCert(t)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)

	pem, err := os.ReadFile(certFile)
	require.NoError(t, err)

	roots = x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pem))

	return serve(t, append([]ext.Option{
		ext.WithServerOption(grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}))),
	}, opts...)...), roots
}

// waitFor reads from ch, failing the test rather than hanging until the package
// timeout if nothing arrives.
func waitFor(t *testing.T, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the server to return")
		return nil
	}
}
