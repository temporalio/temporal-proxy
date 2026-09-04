package cloud

import (
	"net"
	"strings"
)

// endpointSuffix is the domain Temporal Cloud serves its endpoints from.
const endpointSuffix = ".tmprl.cloud"

// APIHostPort is Temporal Cloud's control plane, which serves CloudService. It
// answers the methods Cloud does not serve on a namespace frontend, and is the
// same address the Cloud SDK dials by default.
const APIHostPort = "saas-api" + endpointSuffix + ":443"

// IsEndpoint reports whether hostPort addresses Temporal Cloud. The port is
// optional, and a template action in place of the host is tolerated, since a
// templated address is rendered per request but keeps its domain.
//
// This recognizes per-namespace and regional endpoints. Private-link endpoints
// use per-VPC hostnames that carry no Cloud domain, so those have to be declared
// rather than detected.
func IsEndpoint(hostPort string) bool {
	host := hostPort
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	return strings.HasSuffix(host, endpointSuffix)
}
