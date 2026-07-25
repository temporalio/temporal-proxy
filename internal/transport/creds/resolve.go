package creds

// Mode is the transport-security decision derived once from the configured
// material and the role. It is exposed for logging and tests; callers never
// branch on the raw file paths themselves.
type Mode int

const (
	// ModeInsecure disables transport security.
	ModeInsecure Mode = iota
	// ModeServerTLS presents a server certificate; clients are not required to
	// present one.
	ModeServerTLS
	// ModeSystemTLS verifies the peer against the system root pool (client side).
	ModeSystemTLS
	// ModeCustomCA verifies the peer against a private CA or pinned anchor
	// (client side).
	ModeCustomCA
	// ModeMutualTLS presents a certificate and verifies the peer's certificate.
	ModeMutualTLS
)

// resolveServer computes the inbound (listener) mode for o. A CA requires and
// verifies client certificates (mutual TLS); otherwise the listener presents
// its own certificate (server TLS).
func resolveServer(o *options) Mode {
	switch {
	case o.insecure:
		return ModeInsecure
	case o.ca != "":
		return ModeMutualTLS
	default:
		return ModeServerTLS
	}
}

// resolveClient computes the outbound (dialer) mode for o. A client certificate
// selects mutual TLS; a CA alone verifies the peer against a private anchor;
// with no material the peer is verified against the system root pool.
func resolveClient(o *options) Mode {
	switch {
	case o.insecure:
		return ModeInsecure
	case o.cert != "" || o.key != "":
		return ModeMutualTLS
	case o.ca != "":
		return ModeCustomCA
	default:
		return ModeSystemTLS
	}
}
