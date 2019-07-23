package models

// GitalyServer allows configuring the servers that RPCs are proxied to
type GitalyServer struct {
	Name       string `toml:"name"`
	ListenAddr string `toml:"listen_addr" split_words:"true"`
	Token      string `toml:"token"`
}
