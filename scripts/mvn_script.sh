#!/bin/bash

# Script run on mvn build
echo '[scripts/mvn_script.sh] running'

# Customize configs/zookeeper.json to include hostname
echo '[scripts/mvn_script.sh] Adding hostname to configs/zookeeper.json'
sed -i "s+.*rootPath.*+  \"rootPath\":\"acp_prod/$(hostname)\",+" configs/zookeeper.json

echo '[scripts/mvn_script.sh] completed'

