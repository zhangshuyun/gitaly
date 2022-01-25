package helper

import (
	"fmt"
	"time"

	"github.com/beevik/ntp"
)

// CheckClockSync checks if machine clock has allowed drift threshold compare to NTP service.
// ntpHost is a URL of the NTP service to query, if not set the default pool.ntp.org is used.
// driftThreshold is a time duration that is considered acceptable time offset.
func CheckClockSync(ntpHost string, driftThreshold time.Duration) (bool, error) {
	if ntpHost == "" {
		ntpHost = "pool.ntp.org"
	}

	resp, err := ntp.Query(ntpHost)
	if err != nil {
		return false, fmt.Errorf("query ntp: %w", err)
	}
	if err := resp.Validate(); err != nil {
		return false, fmt.Errorf("validate ntp response: %w", err)
	}

	if resp.ClockOffset > driftThreshold || resp.ClockOffset < -driftThreshold {
		return false, nil
	}

	return true, nil
}
