package env

import (
	"fmt"
	"os"
	"strconv"
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
