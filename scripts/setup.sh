#!/bin/bash

params='{
		"duration": "3h",
		"retention": "24h",
		"resolution": "1m",
		"maxROEpochs": 2,
		"maxRWEpochs": 2
}'

mkdir -p /tmp/data/test/
echo $params > /tmp/data/test/params.json
