package config

type (
	// ListenConfig defines properties for an inbound listener.
	ListenConfig struct {
		HostPort string     `yaml:"hostPort"`
		TLS      *TLSConfig `yaml:"tls"`
	}

	// TLSConfig specifies TLS material for an inbound HTTPS listener. When CAFile
	// is non-empty the listener enforces mutual TLS: connecting clients must
	// present a certificate signed by that CA.
	//
	// NB: Be sure to set ServerName when the host name you dial doesn't match the
	// CN or SAN on the server's certificate.
	TLSConfig struct {
		CAFile     string `yaml:"ca"`         // PEM-encoded CA certificate (mTLS only)
		CertFile   string `yaml:"cert"`       // PEM-encoded server certificate
		KeyFile    string `yaml:"key"`        // PEM-encoded private key
		ServerName string `yaml:"serverName"` // Optional SNI override
	}
)
