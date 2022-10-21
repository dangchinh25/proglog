package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// TLSConfig defines the parameters that SetupTLSConfig uses to determine what type of *tls.Config to return
type TLSConfig struct {
	CertFile      string // Server certificate file path
	KeyFile       string // Private key file path
	CAFile        string // Root CA certificate file path
	ServerAddress string
	IsServer      bool
}

// SetupTLSConfig takes in parameter and returns different type of tlsConfig, convenient for testing.
// Client tlsConfig is setup to verify the server's certificate by setting the RootCAs, and allow the server to verify the client's certificate by setting its RootCAs and its certificates.
// Server tlsConfig is setup to verify the client's certificate and allow the client to verify the server's certificate by setting its ClientCAs,
// certificate, and ClientAtuth mode set to tls.RequireAndVerifyCert
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		// Read and parse root self-issued CA file
		b, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q", cfg.CAFile,
			)
		}
		// Setup config based on server type
		if cfg.IsServer {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}
