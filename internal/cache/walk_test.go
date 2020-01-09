package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestcleanWalkDirNotExists(t *testing.T) {
	err := cleanWalk("/path/that/does/not/exist")
	assert.NoError(t, err, "cleanWalk returned an error for a non existing directory")
}
