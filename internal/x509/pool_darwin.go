package x509

import (
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"
)

// SystemCertPool circumvents the fact that Go on macOS does not support
// SSL_CERT_{DIR,FILE}.
func SystemCertPool() (*x509.CertPool, error) {
	var certPem []byte

	if f := os.Getenv(SSLCertFile); len(f) > 0 {
		pem, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}

		pem = append(pem, '\n')
		certPem = append(certPem, pem...)
	}

	if d := os.Getenv(SSLCertDir); len(d) > 0 {
		entries, err := os.ReadDir(d)
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			pem, err := os.ReadFile(filepath.Join(d, entry.Name()))
			if err != nil {
				return nil, err
			}

			pem = append(pem, '\n')
			certPem = append(certPem, pem...)
		}
	}

	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	if !pool.AppendCertsFromPEM(certPem) {
		return nil, errors.New("certificate(s) can't be added to the system pool")
	}
	return pool, nil
}
