package env

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// GetBool fetches and parses a boolean typed environment variable
//
// If the variable is empty, returns `fallback` and no error.
// If there is an error, returns `fallback` and the error.
func GetBool(name string, fallback bool) (bool, error) {
	s := os.Getenv(name)
	if s == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return fallback, fmt.Errorf("get bool %s: %w", name, err)
	}
	return v, nil
}

// GetInt fetches and parses an integer typed environment variable
//
// If the variable is empty, returns `fallback` and no error.
// If there is an error, returns `fallback` and the error.
func GetInt(name string, fallback int) (int, error) {
	s := os.Getenv(name)
	if s == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return fallback, fmt.Errorf("get int %s: %w", name, err)
	}
	return v, nil
}

// GetDuration fetches and parses a duration typed environment variable
func GetDuration(name string, fallback time.Duration) (time.Duration, error) {
	s := os.Getenv(name)
	if s == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fallback, fmt.Errorf("get duration %s: %w", name, err)
	}
	return v, nil
}
