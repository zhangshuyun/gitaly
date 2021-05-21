#!/bin/bash

# This script is used to test that a command writes at most maxBytes to stderr. It simulates the
# edge case where the logwriter has already written MaxStderrBytes-1 (9999) bytes

# This edge case happens when 9999 bytes are written. To simulate this, stderr_max_bytes_edge_case has 4 lines of the following format:
# line1: 3333 bytes long
# line2: 3331 bytes
# line3: 3331 bytes
# line4: 1 byte
# The first 3 lines sum up to 9999 bytes written, since we write a 2-byte escaped `\n` for each \n we see.
# The 4th line can be any data.

printf 'a%.0s' {1..3333} >&2
printf '\n' >&2
printf 'a%.0s' {1..3331} >&2
printf '\n' >&2
printf 'a%.0s' {1..3331} >&2
printf '\na\n' >&2
exit 1
