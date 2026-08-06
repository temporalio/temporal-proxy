// Package services names the gRPC services the proxy can forward and resolves
// their descriptors.
//
// Importing this package links each service's generated code into the binary,
// which is what registers its descriptors with protoregistry.GlobalFiles.
// Resolution elsewhere in the proxy (namespace extraction, the reflective
// forwarder, configuration validation) reads that global registry, so a service
// absent here cannot be routed or forwarded no matter what configuration asks
// for.
package services
