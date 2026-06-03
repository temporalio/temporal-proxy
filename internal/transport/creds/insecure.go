package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Insecure disables transport security on both client and server
// connections. No encryption or certificate verification is performed.
//
// Use this only in trusted environments (e.g. local development, loopback
// connections) where TLS is handled at another layer such as a service mesh.
type Insecure struct{}

// NewInsecure returns an [Insecure] that disables transport security.
func NewInsecure() *Insecure {
	return new(Insecure)
}

// DialOption returns a [grpc.DialOption] that disables transport security for
// outbound connections.
func (c *Insecure) DialOption() (grpc.DialOption, error) {
	return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
}

// ServerOption returns a [grpc.ServerOption] that disables transport security
// for inbound connections.
func (c *Insecure) ServerOption() (grpc.ServerOption, error) {
	return grpc.Creds(insecure.NewCredentials()), nil
}

// Validate reports any configuration problems with this credential. Insecure
// has no certificates or keys to inspect, so it always succeeds.
func (c *Insecure) Validate() error {
	return nil
}
