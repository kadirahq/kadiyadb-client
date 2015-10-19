#!/bin/bash

params='{
		"duration": 10800000000000,
		"retention": 108000000000000,
		"resolution": 60000000000,
		"maxROEpochs": 2,
		"maxRWEpochs": 2
}'

mkdir -p /tmp/data/test/
echo $params > /tmp/data/test/params.json
