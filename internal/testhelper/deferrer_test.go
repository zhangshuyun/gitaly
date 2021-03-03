package testhelper

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeferrer(t *testing.T) {
	t.Run("nothing to call", func(t *testing.T) {
		var deferrer Deferrer
		deferrer.Call()
	})

	t.Run("call executes in right order", func(t *testing.T) {
		var deferrer Deferrer

		calls := make(map[string]int)
		deferrer.Add(func() { calls["right"] = len(calls) })
		deferrer.Add(func() { calls["is"] = len(calls) })
		deferrer.Add(func() { calls["order"] = len(calls) })

		deferrer.Call()
		require.Equal(t, map[string]int{"order": 0, "is": 1, "right": 2}, calls)
	})

	t.Run("reset", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			var deferrer Deferrer
			deferrer.reset()
			deferrer.Call()
		})

		t.Run("not empty", func(t *testing.T) {
			var deferrer Deferrer
			var updated bool
			deferrer.Add(func() { updated = true })
			deferrer.reset()
			deferrer.Call()
			require.False(t, updated)
		})
	})

	t.Run("clone", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			var deferrer Deferrer
			cloned := deferrer.clone()
			deferrer.Call()
			cloned.Call()
		})

		t.Run("not empty", func(t *testing.T) {
			var deferrer Deferrer
			var called int
			deferrer.Add(func() { called++ })
			cloned := deferrer.clone()
			deferrer.Call()
			require.Equal(t, called, 1)
			cloned.Call()
			require.Equal(t, called, 2)
		})
	})

	t.Run("Relocate", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			var deferrer Deferrer
			relocated := deferrer.Relocate()
			deferrer.Call()
			relocated.Call()
		})

		t.Run("not empty", func(t *testing.T) {
			var deferrer Deferrer
			var called int
			deferrer.Add(func() { called++ })
			relocated := deferrer.Relocate()
			deferrer.Call()
			require.Equal(t, called, 0)
			relocated.Call()
			require.Equal(t, called, 1)
		})
	})
}
