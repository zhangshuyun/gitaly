package storage

// ServerInfo contains information about how to reach a Gitaly server or a
// Praefect virtual storage. This is necessary for Gitaly RPC's involving more
// than one Gitaly. Without this information, Gitaly would not know how to reach
// the remote peer.
type ServerInfo struct {
	Address string `json:"address"`
	Token   string `json:"token"`
}

// GitalyServers hold Gitaly servers info like {"default":{"token":"x","address":"y"}},
// to be passed in `gitaly-servers` metadata.
type GitalyServers map[string]ServerInfo
