// Package app assembles the proxy process in one place: the full module graph,
// what feeds it, and how it starts and stops. The binary and the test harness
// both build through [New], so neither can drift from the other.
package app
