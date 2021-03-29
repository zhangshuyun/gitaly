package main

import (
	"errors"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/go-enry/go-license-detector/v4/licensedb/filer"
	log "github.com/sirupsen/logrus"
)

type stubFiler struct{}

func (f *stubFiler) ReadFile(path string) (content []byte, err error) {
	return []byte{}, nil
}

func (f *stubFiler) ReadDir(path string) ([]filer.File, error) {
	return []filer.File{
		{Name: "LICENSE", IsDir: false},
	}, nil
}

func (f *stubFiler) Close() {}

func (f *stubFiler) PathsAreAlwaysSlash() bool {
	return true
}

func preloadLicenseDatabase() {
	// the first call to `licensedb.Detect` could be too long
	// https://github.com/go-enry/go-license-detector/issues/13
	// this is why we're calling it here to preload license database
	// on server startup to avoid long initialization on gRPC
	// method handling.
	_, err := licensedb.Detect(&stubFiler{})
	if err != nil && !errors.Is(err, licensedb.ErrNoLicenseFound) {
		log.WithError(err).Error("Failed to initialize license database")
		return
	}
	log.Info("License database preloaded")
}
