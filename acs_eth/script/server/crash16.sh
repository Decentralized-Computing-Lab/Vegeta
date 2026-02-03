#!/bin/bash

ARR=(0 1 10 11 20 21 30 31 40 41 50 51 60 70 80 90)
SUM=16

for ((i = 0; i < SUM; i++)); do
{
        id=${ARR[$i]}
        echo $id
        host1=$(jq ".nodes[$id].PublicIpAddress" nodes.json)
        host=${host1//\"/}
        port=5000
        user='ubuntu'
        key="~/.ssh/aws"
        node="node"$id

        expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "cd;cd themix/script;./stop.sh main"
expect EOF
exit
END
} &
done

wait
