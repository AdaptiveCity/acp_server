#!/bin/bash

#
# List running ACP processes
#
ps aux | awk '/vertx/ && !/awk/ {print $1,$2,$13,$(NF-1),$(NF)}' | sort -k 4,4 | column -t


