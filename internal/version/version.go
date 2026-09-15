// Package version holds the build identity stamped into the binary at link
// time. It lives here rather than in package main so that code below the entry
// point can read it: the proxy sends its version to Temporal Cloud on every
// outbound request, and nothing on that path can import main.
//
// The values are plain variables because the linker's -X flag can only write to
// one, not to a constant. For the same reason each is initialized with a string
// literal: -X silently does nothing to a variable whose initializer calls a
// function, so a computed default would leave the flag with no effect. They are
// never assigned at run time, so reading them concurrently is safe.
//
// A build that passes no flags keeps the defaults below, which is what a
// developer build reports.
package version

// NB: These are set at build time by the CI/CD process. See .goreleaser.yaml.
var (
	// Version is the release the binary was built from, or "local" when it was
	// built without the release flags.
	Version string = "local"

	// SHA is the git commit the binary was built from.
	SHA string = "unknown"

	// BuildTime is when the binary was built, in RFC 3339.
	BuildTime string = "unknown"
)
