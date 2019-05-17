package main

// #include <libproc.h>
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"unsafe"
)

func procPath(pid int) (string, error) {
	// MacOS does not implement procfs, this simple function calls proc_pidpath from MacOS libproc
	// https://opensource.apple.com/source/xnu/xnu-2422.1.72/libsyscall/wrappers/libproc/libproc.h.auto.html
	// this is just for testing purpose as we do not support MacOS as a production environment

	buf := C.CString(string(make([]byte, C.PROC_PIDPATHINFO_MAXSIZE)))
	defer C.free(unsafe.Pointer(buf))

	if ret, err := C.proc_pidpath(C.int(pid), unsafe.Pointer(buf), C.PROC_PIDPATHINFO_MAXSIZE); ret <= 0 {
		return "", fmt.Errorf("failed process path retrieval: %v", err)
	}

	return C.GoString(buf), nil
}
