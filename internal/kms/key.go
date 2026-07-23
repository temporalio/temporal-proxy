package kms

import (
	"context"
	"fmt"
	"strings"

	"gocloud.dev/secrets"
	_ "gocloud.dev/secrets/awskms"
	_ "gocloud.dev/secrets/azurekeyvault"
	_ "gocloud.dev/secrets/gcpkms"
	_ "gocloud.dev/secrets/localsecrets"
)

// kek adapts a gocloud.dev secrets.Keeper to the crypto.KEK interface. The
// embedded Keeper supplies Encrypt, Decrypt, and Close; kek adds the ID.
type kek struct {
	*secrets.Keeper
	id string
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

func safeKeyString(uri string) string {
	if !strings.HasPrefix(uri, "testing://") {
		return uri
	}

	return "base64key://<redacted>"
}
