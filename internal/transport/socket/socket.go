// Package socket defines the addressing contract for the proxy's local unix
// socket. UnixPath derives the path once, from the upstream host:port, and the
// caller hands that same value to both the proxy, which listens on it, and the
// dialer, which connects to it. It depends on no other internal packages.
package socket

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// maxUnixPath is a conservative cap on a unix socket path length. The OS limit
// on sockaddr_un.sun_path is 104 bytes on macOS/BSD and 108 on Linux including
// the null terminator; 103 is safe across both.
const maxUnixPath = 103

// UnixPath derives a stable, absolute unix socket path for hostPort, placed
// under os.TempDir(). It is deterministic for a given hostPort within a
// process, so a caller that derives it once and passes the same string to both
// the listener and the dialer never has the two disagree. It returns an error
// when the resulting path would exceed the platform's sun_path limit, rather
// than letting the OS silently truncate it (which would break that agreement).
func UnixPath(hostPort string) (string, error) {
	sum := sha256.Sum256([]byte(hostPort))
	hash := hex.EncodeToString(sum[:])[:8]
	slug := strings.Map(func(r rune) rune {
		if r == ':' || r == '.' || r == '/' {
			return '-'
		}
		return r
	}, hostPort)

	path := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.sock", slug[:min(32, len(slug))], hash))
	if err := ValidatePath(path); err != nil {
		return "", err
	}

	return path, nil
}

// ValidatePath returns an error when path would exceed the platform's sun_path
// limit. Callers that supply their own socket path instead of deriving one with
// [UnixPath] use it to fail fast rather than at bind time.
func ValidatePath(path string) error {
	if len(path) > maxUnixPath {
		return fmt.Errorf("unix socket path %q (%d bytes) exceeds limit of %d", path, len(path), maxUnixPath)
	}

	return nil
}
