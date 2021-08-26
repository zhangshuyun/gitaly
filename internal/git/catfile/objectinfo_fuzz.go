//go:build gofuzz
// +build gofuzz

package catfile

import (
	"bufio"
	"bytes"
)

func Fuzz(data []byte) int {
	reader := bufio.NewReader(bytes.NewReader(data))
	ParseObjectInfo(reader)
	return 0
}
