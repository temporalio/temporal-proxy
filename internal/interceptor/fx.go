package interceptor

import (
	"fmt"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/proxy"
)

// Module is the fx module that provides the proxy's outbound client
// interceptors. It builds a single payload interceptor from the optional
// [PayloadCodec] chain in [InterceptorParams] and provides it as a
// []grpc.UnaryClientInterceptor for the proxy to chain onto its upstream
// connection.
var Module = fx.Options(fx.Provide(
	fx.Annotate(func(p InterceptorParams) ([]grpc.UnaryClientInterceptor, error) {
		payloads, err := Payloads(p.Codecs...)
		if err != nil {
			return nil, fmt.Errorf("failed to construct payloads interceptor: %w", err)
		}

		return []grpc.UnaryClientInterceptor{payloads}, nil
	}, proxy.UnaryInterceptorsTag),
	fx.Annotate(func(_ InterceptorParams) ([]grpc.StreamClientInterceptor, error) {
		return nil, nil
	}, proxy.StreamInterceptorsTag),
))

// InterceptorParams collects the fx-provided dependencies used to build the
// proxy's interceptors. Codecs is optional; when none are supplied the payload
// interceptor is a passthrough that leaves payloads unchanged.
type InterceptorParams struct {
	fx.In

	Codecs []PayloadCodec `optional:"true"`
}
