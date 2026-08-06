// Package services names the gRPC services the proxy can forward and resolves
// their descriptors.
//
// Importing this package links each service's generated code into the binary,
// which is what registers its descriptors with protoregistry.GlobalFiles.
// Resolution elsewhere in the proxy (namespace extraction, the reflective
// forwarder, configuration validation) reads that global registry, so a service
// absent here cannot be routed or forwarded no matter what configuration asks
// for.
//
// It depends on no other internal package, deliberately: the namespace
// completeness guard is an internal test in package protoutil and imports this
// package, so a dependency on protoutil here would be an import cycle.
package services
