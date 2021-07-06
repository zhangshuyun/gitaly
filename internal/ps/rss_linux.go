package ps

import (
	"fmt"
	"os"
)

var pageSize = os.Getpagesize()

// https://gitlab.com/procps-ng/procps/-/blob/37f106029975e3045b0cd779525d14c55d24b74e/proc/readproc.h#L51
// https://man7.org/linux/man-pages/man5/proc.5.html
type statm struct {
	size, resident, shared, text, lib, data, dt int
}

// RSS returns the RSS of a process, in kB
func RSS(pid int) (int, error) {
	file, err := os.Open(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	s := statm{}

	// unit for each of these is pages
	// https://gitlab.com/procps-ng/procps/-/blob/37f106029975e3045b0cd779525d14c55d24b74e/proc/readproc.c#L660
	_, err = fmt.Fscanf(file, "%d %d %d %d %d %d %d",
		&s.size, &s.resident, &s.shared,
		&s.text, &s.lib, &s.data, &s.dt)
	if err != nil {
		return 0, err
	}

	rssKbytes := (s.resident * pageSize) / 1024

	return rssKbytes, nil
}
