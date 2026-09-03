// Package cloud holds the rules that are specific to Temporal Cloud rather than
// to any Temporal Service.
//
// Today that means namespace validation. Cloud identifies a namespace as
// "<name>.<account-id>", a shape self-hosted deployments do not impose, so
// [ValidateNamespace] checks a string against it before the proxy uses that
// string to address a Cloud upstream.
package cloud
