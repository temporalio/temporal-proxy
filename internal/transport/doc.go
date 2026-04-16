// Package transport implements a multiplexed gRPC transport layer for the Temporal proxy.
//
// # Overview
//
// Each cluster connection is backed by a pool of TCP connections, each carrying multiple
// logical gRPC streams via [yamux] session multiplexing. The pool is managed by [Mux], which
// continuously dials or accepts connections and hands them to [GRPCMux]. GRPCMux wraps each
// raw connection in a [Session], which health-checks the underlying yamux session and notifies
// registered [SessionListener]s when the active set changes. [ClientConn] implements
// [grpc.ClientConnInterface] and uses gRPC's built-in round-robin load balancer to distribute
// calls across all live sessions.
//
// # NAT traversal
//
// A key motivation for this design is enabling cross-cluster communication without requiring
// every participant to accept inbound TCP connections. When a remote cluster is of type
// [Inbound], the remote side dials *out* to this proxy (no open ingress ports required on the
// remote). Because yamux supports bidirectional stream multiplexing over a single TCP
// connection, the proxy can open new streams back to the remote over the same connection,
// effectively allowing full duplex gRPC communication through a single outbound dial.
//
// # Connection directions
//
// [MuxKind] controls the role each side plays:
//
//   - [Inbound]  – this proxy listens and accepts incoming connections from the remote.
//   - [Outbound] – this proxy dials the remote address.
//
// [yamux]: https://github.com/hashicorp/yamux
package transport
