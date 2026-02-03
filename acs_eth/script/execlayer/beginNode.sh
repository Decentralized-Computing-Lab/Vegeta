#!/bin/bash

NUM=$1

for ((i = 0; i < NUM; i++)); do
{
  host1=$(jq ".nodes[$i].PublicIpAddress" clients.json)
  host=${host1//\"/}
  url1=$(jq ".nodes[$i].ServerURL" clients.json)
  url=${url1//\"/}
  port=8000
  user='ubuntu'
  key="~/.ssh/eth_parallel.pem"
  id=$i
  node="node"$id

expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "cd;cd execlayer;nohup ./execlayer -num $NUM -id $i > output 2>&1 &"
expect EOF
exit
END
} &
done

wait
