#!/bin/bash

printf 'a%.0s' {1..8192} >&2
printf '\n' >&2
printf 'b%.0s' {1..8192} >&2
exit 1
