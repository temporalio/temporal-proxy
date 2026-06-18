package protoutil

import (
	"context"
	"fmt"
	"reflect"
)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
)

type (
	// Service describes a gRPC service interface: its Go type name, the
	// reflected interface type it was parsed from, and the RPCs declared on it.
	Service struct {
		// Name is the interface's type name, e.g. "WorkflowServiceServer".
		Name string
		// Type is the reflected interface type the service was parsed from.
		Type reflect.Type
		// RPCs are the service's unary RPC methods, in method-name order.
		RPCs []RPC
	}

	// RPC describes a single unary RPC method on a service interface.
	RPC struct {
		// Name is the method name, e.g. "StartWorkflowExecution".
		Name string
		// Unary reports whether the method is a unary RPC. ParseService only
		// records unary RPCs, so this is always true for the RPCs it returns.
		Unary bool
		// Req is the request message struct type: the element type of the
		// method's *Request parameter.
		Req reflect.Type
		// Resp is the response message struct type: the element type of the
		// method's *Response result.
		Resp reflect.Type
	}
)

// ParseService reflects over a gRPC server interface type and returns the unary
// RPCs declared on it, in method-name order. Non-unary methods, such as
// streaming RPCs and the generated mustEmbedUnimplementedXServer bookkeeping
// method, are skipped; see IsUnaryRPC. The error result is currently always
// nil and is reserved for future validation.
func ParseService(t reflect.Type) (*Service, error) {
	rpcs := make([]RPC, 0, t.NumMethod())
	for m := range t.Methods() {
		if IsUnaryRPC(m.Type) {
			rpcs = append(rpcs, RPC{
				Name:  m.Name,
				Unary: true,
				Req:   m.Type.In(1).Elem(),
				Resp:  m.Type.Out(0).Elem(),
			})
		}
	}

	return &Service{
		Name: t.Name(),
		Type: t,
		RPCs: rpcs,
	}, nil
}

// IsUnaryRPC reports whether t is the signature of a unary RPC method as it
// appears on a gRPC server interface: func(context.Context, *Request)
// (*Response, error). Interface method types carry no receiver, so the context
// is the first parameter. Variadic methods, streaming RPCs, and non-RPC methods
// (e.g. mustEmbedUnimplementedXServer) do not match.
func IsUnaryRPC(t reflect.Type) bool {
	return !t.IsVariadic() &&
		t.NumIn() == 2 && t.In(0) == contextType && t.In(1).Kind() == reflect.Pointer &&
		t.NumOut() == 2 && t.Out(0).Kind() == reflect.Pointer && t.Out(1) == errorType
}

// MessageTypes returns the de-duplicated request and response message struct
// types of the service's RPCs, for use as message-graph roots. It returns an
// error if the service has no RPCs.
func (s *Service) MessageTypes() ([]reflect.Type, error) {
	seen := map[reflect.Type]bool{}
	var roots []reflect.Type

	for _, rpc := range s.RPCs {
		for _, t := range []reflect.Type{rpc.Req, rpc.Resp} {
			if !seen[t] {
				seen[t] = true
				roots = append(roots, t)
			}
		}
	}

	if len(roots) == 0 {
		return nil, fmt.Errorf("no unary RPC request/response types found on %s", s.Name)
	}

	return roots, nil
}
