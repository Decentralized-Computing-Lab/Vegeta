#!/bin/bash

NUM=$1

for ((i = 0; i < NUM; i++)); do
{
  host1=$(jq ".nodes[$i].PublicIpAddress" ./clients.json)
  host=${host1//\"/}
  port=5000
  user='ubuntu'
  key="~/.ssh/eth_parallel.pem"
  id=$i
  node="node"$id

  expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "cd;mkdir ACSclient"
expect EOF
exit
END

  expect -c "
set timeout -1
spawn scp -i $key  ../../src/client/ACSclient $user@$host:ACSclient/
expect 100%
exit
"
} &
done

wait
