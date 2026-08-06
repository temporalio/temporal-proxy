package protoutil

import "strings"

// ServiceName returns the service portion of a gRPC full method
// ("/pkg.Service/Method"), with or without the leading slash gRPC supplies. It
// returns "" when fullMethod carries no method to strip, which callers must
// treat as "no service" rather than as a name to match on.
func ServiceName(fullMethod string) string {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return ""
	}

	return trimmed[:slash]
}
