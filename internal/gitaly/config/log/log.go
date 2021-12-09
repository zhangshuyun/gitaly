package log

// Config contains logging configuration values
type Config struct {
	Dir    string `toml:"dir,omitempty"`
	Format string `toml:"format,omitempty"`
	Level  string `toml:"level,omitempty"`
}
