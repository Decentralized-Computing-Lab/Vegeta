#!/bin/bash

NUM=$1
NAME=$2

for ((i = 0; i < NUM; i++)); do
{
  host1=$(jq ".nodes[$i].PublicIpAddress" clients.json)
  host=${host1//\"/}
  port=5000
  user='ubuntu'
  key="~/.ssh/aws"
  id=$i
  node="node"$id
  expect -c "
set timeout -1
spawn scp -i $key $user@$host:client/client.log ./log/client$id/$NAME
expect 100%
exit
"
} &
done

wait
