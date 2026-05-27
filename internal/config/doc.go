// Package config loads and validates the codec-server YAML configuration.
// Use Load to parse from an io.Reader or LoadFile to read from disk. Both
// expand ${VAR} references using the process environment before unmarshalling.
package config
