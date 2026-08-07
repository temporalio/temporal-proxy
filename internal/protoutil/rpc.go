package protoutil

import "strings"

// SplitMethod splits a gRPC full method ("/pkg.Service/Method", with or without
// the leading slash gRPC supplies) into its service and method halves. It
// reports false when fullMethod carries no method to split off, in which case
// both halves are empty.
func SplitMethod(fullMethod string) (service, method string, ok bool) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return "", "", false
	}

	return trimmed[:slash], trimmed[slash+1:], true
}

// ServiceName returns the service portion of a gRPC full method
// ("/pkg.Service/Method"), with or without the leading slash gRPC supplies. It
// returns "" when fullMethod carries no method to strip, which callers must
// treat as "no service" rather than as a name to match on.
func ServiceName(fullMethod string) string {
	service, _, _ := SplitMethod(fullMethod)
	return service
}
