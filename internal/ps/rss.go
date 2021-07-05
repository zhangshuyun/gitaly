// +build !linux

package ps

import (
	"strconv"
)

// RSS invokes ps -o rss= -p pid and returns its output
func RSS(pid int) (int, error) {
	rss, err := Exec(pid, "rss=")
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(rss)
}
