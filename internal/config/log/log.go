package log

// Config contains logging configuration values
type Config struct {
	Dir    string `toml:"dir"`
	Format string `toml:"format"`
	Level  string `toml:"level"`
}
