// +build postgres

package datastore

import (
	"testing"
)

func TestGenerationStore_Postgres(t *testing.T) {
	testGenerationStore(t, func(t *testing.T, storages map[string][]string) GenerationStore {
		return NewPostgresGenerationStore(getDB(t), storages)
	})
}
