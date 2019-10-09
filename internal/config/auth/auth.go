package auth

// Config is a struct for an authentication config
type Config struct {
	Transitioning bool   `toml:"transitioning"`
	Token         string `toml:"token"`
}
