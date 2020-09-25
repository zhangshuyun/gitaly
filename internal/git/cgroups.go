package git

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

var (
	cgroupOnce sync.Once
	cgroupSet  map[string]string
	cgroupErr  error
)

// TODO: instead of discovery here, we could provide it as a
// config flag GITALY_CGROUP_PREFIX, which users can then set
// to something like "/system.slice/gitlab-runsvdir.service".
// that wouold allos us to drop all of this code. but would also
// requires to pre-create cgroups ahead of time
func getCgroupSet() (map[string]string, error) {
	cgroupOnce.Do(func() {
		cgroupSet, cgroupErr = parseCgroupFile("/proc/self/cgroup")
		if cgroupErr != nil {
			return
		}

		cgroupCount := uint64(4096)
		if os.Getenv("GITALY_CGROUP_COUNT") != "" {
			cgroupCount, cgroupErr = strconv.ParseUint(os.Getenv("GITALY_CGROUP_COUNT"), 64, 10)
			if cgroupErr != nil {
				return
			}
		}

		for i := uint64(0); i < cgroupCount; i++ {
			src := make([]byte, 8)
			binary.BigEndian.PutUint64(src, i)

			dst := make([]byte, hex.EncodedLen(len(src)))
			hex.Encode(dst, src)
			dst = dst[len(dst)-3:]

			// TODO: batch these up
			cmd := exec.Command("sudo", "cgcreate", "-g", "cpu:"+cgroupSet["cpu"]+"/"+string(dst), "-g", "memory:"+cgroupSet["memory"]+"/"+string(dst))
			cgroupErr := cmd.Run()
			if cgroupErr != nil {
				return
			}
		}
	})
	return cgroupSet, cgroupErr
}

func parseCgroupFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseCgroupFromReader(f)
}

func parseCgroupFromReader(r io.Reader) (map[string]string, error) {
	var (
		cgroups = make(map[string]string)
		s       = bufio.NewScanner(r)
	)
	for s.Scan() {
		var (
			text  = s.Text()
			parts = strings.SplitN(text, ":", 3)
		)
		if len(parts) < 3 {
			return nil, fmt.Errorf("invalid cgroup entry: %q", text)
		}
		for _, subs := range strings.Split(parts[1], ",") {
			if subs != "" {
				cgroups[subs] = parts[2]
			}
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return cgroups, nil
}
