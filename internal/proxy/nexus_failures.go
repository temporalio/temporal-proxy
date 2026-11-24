package proxy

import (
	"context"

	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"go.temporal.io/proxy/pkg/codec"
)

// nexusTemporalFailure is the metadata type the Temporal SDKs give a Nexus
// failure whose details hold a serialized Temporal failure.
var nexusTemporalFailure = string((&failure.Failure{}).ProtoReflect().Descriptor().FullName())

// legacyNexusFailures handles the failures Nexus handlers send in the
// deprecated nexus.v1.Failure shape, which SDKs fall back to when the server
// lacks the TemporalFailureResponses capability. Neither visitor reaches them,
// since they hold no Payload and are not Temporal failures, so the Temporal
// failure an SDK serializes into details is decoded and sent through the same
// outbound steps as any other. It is only installed while encrypting; otherwise
// there is nothing to seal and the failure goes upstream as the handler sent it.
type legacyNexusFailures struct {
	codecs   *Codecs
	payloads *proxy.VisitPayloadsOptions
}

// intercept prepares any deprecated Nexus failure in req before sending it.
func (l legacyNexusFailures) intercept(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	for f := legacyNexusFailure(req); f != nil; f = f.GetCause() {
		if err := l.prepare(ctx, f); err != nil {
			return err
		}
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

// prepare treats f as its current-shape equivalent would be treated. A
// Temporal failure in details is decoded and prepared in full. A failure with
// only a message has nowhere to keep it sealed, so with failure encoding on the
// message is replaced outright. Anything else may hold data the proxy cannot
// find, so it is refused rather than sent in plaintext.
func (l legacyNexusFailures) prepare(ctx context.Context, f *nexus.Failure) error {
	switch {
	case f.GetMetadata()["type"] == nexusTemporalFailure:
		return l.prepareTemporal(ctx, f)
	case len(f.GetMetadata()) == 0 && len(f.GetDetails()) == 0 && f.GetCause() == nil:
		if l.codecs.encodeFailures {
			f.Message = codec.EncodedFailureMessage
			f.StackTrace = ""
		}

		return nil
	default:
		return status.Errorf(codes.FailedPrecondition,
			"proxy: cannot encrypt a Nexus failure of type %q sent in the deprecated format; "+
				"upgrade the handler's Temporal SDK or the upstream server", f.GetMetadata()["type"])
	}
}

// prepareTemporal decodes the Temporal failure in f's details, rebuilt as the
// server rebuilds it with the message and stack trace from f, prepares it, and
// serializes it back. Fields this proxy does not know are refused rather than
// dropped, since one could hold a payload it would otherwise miss.
func (l legacyNexusFailures) prepareTemporal(ctx context.Context, f *nexus.Failure) error {
	var tf failure.Failure
	if err := protojson.Unmarshal(f.GetDetails(), &tf); err != nil {
		return status.Errorf(codes.InvalidArgument, "proxy: cannot decode the Temporal failure in a Nexus failure: %v", err)
	}

	tf.Message = f.GetMessage()
	tf.StackTrace = f.GetStackTrace()

	err := proxy.VisitFailures(ctx, &tf, proxy.VisitFailuresOptions{
		Visitor: func(_ *proxy.VisitFailuresContext, x *failure.Failure) error { return l.codecs.sendFailure(x) },
	})
	if err != nil {
		return err
	}

	if l.payloads != nil {
		if err := proxy.VisitPayloads(ctx, &tf, *l.payloads); err != nil {
			return err
		}
	}

	f.Message, f.StackTrace = tf.GetMessage(), tf.GetStackTrace()
	tf.Message, tf.StackTrace = "", ""

	details, err := protojson.Marshal(&tf)
	if err != nil {
		return status.Errorf(codes.Internal, "proxy: cannot encode the Temporal failure in a Nexus failure: %v", err)
	}

	f.Details = details

	return nil
}

// legacyNexusFailure returns the deprecated Nexus failure req carries, if any.
func legacyNexusFailure(req any) *nexus.Failure {
	switch r := req.(type) {
	case *workflowservice.RespondNexusTaskFailedRequest:
		return r.GetError().GetFailure() //nolint:staticcheck // the deprecated field is the one handled here
	case *workflowservice.RespondNexusTaskCompletedRequest:
		return r.GetResponse().GetStartOperation().GetOperationError().GetFailure() //nolint:staticcheck // as above
	default:
		return nil
	}
}
