/*
   Adapted from https://github.com/containerd/cgroups/blob/f1d9380fd3c028194db9582825512fdf3f39ab2a/mock_test.go

   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package cgroups

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/cgroups"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

type mockCgroup struct {
	root       string
	subsystems []cgroups.Subsystem
}

func newMock(t *testing.T) *mockCgroup {
	t.Helper()

	root := testhelper.TempDir(t)

	subsystems, err := defaultSubsystems(root)
	require.NoError(t, err)

	for _, s := range subsystems {
		require.NoError(t, os.MkdirAll(filepath.Join(root, string(s.Name())), os.FileMode(0755)))
	}

	return &mockCgroup{
		root:       root,
		subsystems: subsystems,
	}
}

func (m *mockCgroup) hierarchy() ([]cgroups.Subsystem, error) {
	return m.subsystems, nil
}
