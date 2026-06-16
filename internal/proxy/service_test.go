package proxy_test

import (
	"context"
	"reflect"
)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
)

// rpcMethodNames returns the names of every unary (for now) RPC method on pt.
func rpcMethodNames(pt reflect.Type) []string {
	names := make([]string, 0, pt.NumMethod())
	for m := range pt.Methods() {
		// m.Func includes the receiver as In(0); strip it for the shape check.
		ft := reflect.FuncOf(
			inputs(m.Func.Type()),
			outputs(m.Func.Type()),
			m.Func.Type().IsVariadic(),
		)

		// TODO: Support streaming when we need it.
		if isUnaryRPC(ft) {
			names = append(names, m.Name)
		}
	}

	return names
}

// isUnaryRPC reports whether a bound proxy method has the shape every forwarded
// RPC shares: func(context.Context, *Req) (*Resp, error). This filters out the
// embedded UnimplementedWorkflowServiceServer helpers (e.g. mustEmbed...) and
// any non-RPC method, so the table below covers exactly the proxied calls and
// automatically picks up new ones as they are added.
func isUnaryRPC(t reflect.Type) bool {
	return !t.IsVariadic() &&
		t.NumIn() == 2 && t.In(0) == contextType && t.In(1).Kind() == reflect.Pointer &&
		t.NumOut() == 2 && t.Out(0).Kind() == reflect.Pointer && t.Out(1) == errorType
}

// inputs returns t's parameter types excluding the leading receiver.
func inputs(t reflect.Type) []reflect.Type {
	in := make([]reflect.Type, 0, t.NumIn()-1)
	for i := 1; i < t.NumIn(); i++ {
		in = append(in, t.In(i))
	}

	return in
}

// outputs returns t's result types.
func outputs(t reflect.Type) []reflect.Type {
	out := make([]reflect.Type, 0, t.NumOut())
	for res := range t.Outs() {
		out = append(out, res)
	}

	return out
}
