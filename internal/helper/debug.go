package helper

import (
	"log"
	"os"
)

// Debugf behaves similarly to log.Printf. No-op unless GITALY_DEBUG=1.
func Debugf(format string, args ...interface{}) {
	if os.Getenv("GITALY_DEBUG") != "1" {
		return
	}

	log.Printf("debug: "+format, args...)
}
