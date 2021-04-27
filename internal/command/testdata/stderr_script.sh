#!/bin/bash

for i in {1..5}
do
	echo 'hello world' 1>&2
done
exit 1
