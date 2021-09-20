package packfile

import (
	"os"
	"path/filepath"
)

// List returns the packfiles in objDir.
func List(objDir string) ([]string, error) {
	packDir := filepath.Join(objDir, "pack")
	entries, err := os.ReadDir(packDir)
	if err != nil {
		return nil, err
	}

	var packs []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}

		if p := filepath.Join(packDir, ent.Name()); packFileRegex.MatchString(p) {
			packs = append(packs, p)
		}
	}

	return packs, nil
}
