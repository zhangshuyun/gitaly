//go:build darwin || dragonfly || freebsd
// +build darwin dragonfly freebsd

package fstype

import "golang.org/x/sys/unix"

func detectFileSystem(path string) string {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return unknownFS
	}

	var buf []byte
	for _, c := range stat.Fstypename {
		if c == 0 {
			break
		}
		buf = append(buf, c)
	}

	if len(buf) == 0 {
		return unknownFS
	}

	return string(buf)
}
