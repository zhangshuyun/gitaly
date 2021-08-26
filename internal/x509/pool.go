//go:build !darwin
// +build !darwin

package x509

import "crypto/x509"

// SystemCertPool has an override on macOS.
func SystemCertPool() (*x509.CertPool, error) { return x509.SystemCertPool() }
