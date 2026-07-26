// Package certs provides reusable [validation.Check] building blocks for
// inspecting X.509 certificates and PEM material: expiry, CA basic constraint,
// signature-algorithm and key-type strength, and key size. ValidatePEM and its
// file variants parse PEM data and run a set of checks against every certificate
// they contain, aggregating failures into a [validation.Errors].
package certs
