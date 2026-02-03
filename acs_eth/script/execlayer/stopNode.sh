#!/bin/bash

NUM=$1

cp ../server/nodes.json nodes.json

for ((i = 0; i < NUM; i++)); do
{
        host1=$(jq ".nodes[$i].PublicIpAddress" nodes.json)
        host=${host1//\"/}
        port=5000
        user='ubuntu'
        key="~/.ssh/eth_parallel.pem"
        id=$i
        node="node"$id

        expect -c "
set timeout -1
spawn scp -i $key stop.sh $user@$host:ACSserver/script/
expect 100%
exit
"

        expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "cd;chmod 777 ACSserver/ACSserver;cd ACSserver/script;chmod 777 stop.sh"
expect EOF
exit
END

        expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "cd;cd ACSserver/script;./stop.sh"
expect EOF
exit
END
} &
done

wait