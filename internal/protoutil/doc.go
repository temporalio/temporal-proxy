// Package protoutil provides reflection helpers for inspecting gRPC service
// interfaces and the proto message graphs their RPCs carry.
//
// Generated Temporal gRPC servers are Go interfaces whose methods follow a
// fixed shape: unary RPCs look like func(context.Context, *Request) (*Response,
// error). ParseService reflects over such an interface to enumerate its unary
// RPCs and the request/response message types they carry; IsUnaryRPC exposes
// the same method-shape test on its own. Service.MessageTypes returns the
// de-duplicated set of those request/response types for use as graph roots.
//
// From those roots, BuildGraph walks proto message structs by following their
// message-typed Get* getters (MessageEdges), discovering every reachable
// message type, and ReachesTarget computes which of them can reach a target
// type. Together these let code generators and middleware operate over a
// service and its message graph without hand-listing methods or fields.
package protoutil
