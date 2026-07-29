package kms

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"gocloud.dev/secrets"
	_ "gocloud.dev/secrets/awskms"
	_ "gocloud.dev/secrets/azurekeyvault"
	_ "gocloud.dev/secrets/gcpkms"
	_ "gocloud.dev/secrets/localsecrets"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// ExtensionScheme addresses a key served by a configured extension server, as
// "extension://<server>/<key>". The server names an entry in the config's
// extensionServers list; the key is a proxy-side identifier that distinguishes
// several keys hosted by one server. It is never sent to the server, which
// selects keys by namespace when wrapping and reads the key back out of the
// self-describing ciphertext when unwrapping.
const ExtensionScheme = "extension"

// kek adapts a gocloud.dev secrets.Keeper to the crypto.KEK interface. The
// embedded Keeper supplies Encrypt, Decrypt, and Close; kek adds the ID.
type kek struct {
	*secrets.Keeper
	id string
}

// newKEKForURI opens the key addressed by uri. An extension URI resolves to a
// client on a configured extension server's connection; anything else is opened
// as a cloud KMS keeper.
func newKEKForURI(ctx context.Context, uri *url.URL, conns api.Connections) (crypto.KEK, error) {
	if strings.EqualFold(uri.Scheme, ExtensionScheme) {
		return newExtensionKEK(uri, conns)
	}

	return newKEK(ctx, uri.String())
}

// newExtensionKEK resolves an extension URI to a KMS client on the named
// server's pooled connection. Several keys may share one server, so the whole
// URI (not the server name) is the KEK identity: it is recorded in every DEK
// this key wraps and is what selects the key again on the decrypt path.
//
// The connection is owned by the pool, so the returned client's Close is a
// no-op and multiple KEKs may safely share one server.
func newExtensionKEK(uri *url.URL, conns api.Connections) (crypto.KEK, error) {
	server := uri.Host
	if server == "" {
		return nil, fmt.Errorf("extension key URI must name an extension server: %s", uri)
	}

	conn, ok := conns[server]
	if !ok {
		return nil, fmt.Errorf("unknown extension server %q in key URI: %s", server, uri)
	}

	return api.NewKMS(uri.String(), conn), nil
}

// newKEK opens the KMS key addressed by uri as a KEK. The "testing://" scheme
// is rewritten to gocloud's "base64key://" local keeper so tests and local runs
// need no cloud KMS. The returned kek's ID is the (rewritten) URI.
func newKEK(ctx context.Context, uri string) (*kek, error) {
	if after, ok := strings.CutPrefix(uri, "testing://"); ok {
		uri = "base64key://" + after
	}

	kp, err := secrets.OpenKeeper(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to create key for uri: %s, %w", safeKeyString(uri), err)
	}

	return &kek{
		Keeper: kp,
		id:     uri,
	}, nil
}

// ID returns a unique ID for this KEK, e.g. a KMS ARN.
func (k *kek) ID() string {
	return k.id
}

// Encrypt wraps the DEK using the underlying keeper. The namespace is unused: a
// gocloud keeper addresses a single fixed key, so namespace-based selection has
// already happened by the time this KEK is chosen.
func (k *kek) Encrypt(ctx context.Context, _ string, dek []byte) ([]byte, error) {
	return k.Keeper.Encrypt(ctx, dek)
}

func safeKeyString(uri string) string {
	if !strings.HasPrefix(uri, "testing://") {
		return uri
	}

	return "base64key://<redacted>"
}
