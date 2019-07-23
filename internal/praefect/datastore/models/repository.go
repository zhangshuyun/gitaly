package models

// Repository provides all necessary information to address a repository hosted
// in a specific Gitaly replica
type Repository struct {
	RelativePath string `toml:"relative_path"` // relative path of repository
	Storage      string `toml:"storage"`       // storage location, e.g. default
}
