package main

import (
	"os"
	"strconv"
)

func procPath(pid int) (path string, err error) {
	return os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
}
