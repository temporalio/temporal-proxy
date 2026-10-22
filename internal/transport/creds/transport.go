package creds

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
)

// reloadingCreds is client-side gRPC transport credentials that build their TLS
// configuration from the material's current snapshot on every handshake, so a
// reload reaches new connections made with a DialOption taken before it.
type reloadingCreds struct {
	m          *material
	serverName string
}

// ClientHandshake runs the TLS client handshake with the current snapshot.
func (c *reloadingCreds) ClientHandshake(
	ctx context.Context,
	authority string,
	rawConn net.Conn,
) (net.Conn, credentials.AuthInfo, error) {
	return credentials.NewTLS(clientConfig(c.m.current(), c.serverName)).ClientHandshake(ctx, authority, rawConn)
}

// ServerHandshake always fails: these credentials are client-only.
func (*reloadingCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("creds: reloading credentials are client-only")
}

// Info reports the same protocol info as [credentials.NewTLS]. Its ServerName
// becomes the channel authority, which is what the handshake verifies against.
func (c *reloadingCreds) Info() credentials.ProtocolInfo {
	return credentials.NewTLS(&tls.Config{ServerName: c.serverName, MinVersion: minTLSVersion}).Info()
}

// Clone returns a copy that shares the material.
func (c *reloadingCreds) Clone() credentials.TransportCredentials {
	clone := *c

	return &clone
}

// OverrideServerName sets the server name used for verification.
func (c *reloadingCreds) OverrideServerName(name string) error {
	c.serverName = name

	return nil
}

// clientConfig builds the outbound TLS configuration for one handshake: the
// snapshot's CA pool as trust anchors and, for mutual TLS, its certificate.
func clientConfig(s *snapshot, serverName string) *tls.Config {
	cfg := &tls.Config{
		ServerName: serverName,
		MinVersion: minTLSVersion,
		RootCAs:    s.pool,
	}

	if s.cert != nil {
		cfg.Certificates = []tls.Certificate{*s.cert}
	}

	return cfg
}
