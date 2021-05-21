#!/bin/bash

let x=0
while [ $x -lt 100010 ]
do
	let x=x+1
	printf '%06d zzzzzzzzzz\n' $x >&2
done
exit 1
