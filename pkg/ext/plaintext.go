package ext

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// plaintextMessage is logged the first time a call arrives unencrypted, and only
// the first time: an extension server admits callers and unwraps key material, so
// this is worth saying, but not once per request.
const plaintextMessage = "Serving in plaintext. Supply credentials via WithServerOption for production use."

// plaintextWarning returns interceptors that log [plaintextMessage] once if calls
// are arriving over an unencrypted connection.
//
// It reads the connection rather than the configuration because a
// [grpc.ServerOption] is opaque: what [WithServerOption] was handed cannot be read
// back, so whether this server ended up serving TLS is only knowable from a call
// that actually arrived. The cost is that a server nobody ever calls stays quiet,
// which the health service makes unlikely.
func plaintextWarning(log logger.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	var once sync.Once

	warn := func(ctx context.Context) {
		if isPlaintext(ctx) {
			once.Do(func() { log.Warn(plaintextMessage) })
		}
	}

	unary := func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		warn(ctx)

		return handler(ctx, req)
	}

	stream := func(
		srv any,
		ss grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		warn(ss.Context())

		return handler(srv, ss)
	}

	return unary, stream
}

// isPlaintext reports whether the call in ctx arrived unencrypted. A server given
// no credentials attaches no auth information at all, and insecure credentials
// report NoSecurity. A credential that reports no level is left alone rather than
// assumed plaintext, matching how gRPC treats one: a false warning about somebody
// else's custom credentials would be worse than staying quiet.
func isPlaintext(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}

	if p.AuthInfo == nil {
		return true
	}

	common, ok := p.AuthInfo.(interface {
		GetCommonAuthInfo() credentials.CommonAuthInfo
	})
	if !ok {
		return false
	}

	return common.GetCommonAuthInfo().SecurityLevel == credentials.NoSecurity
}
