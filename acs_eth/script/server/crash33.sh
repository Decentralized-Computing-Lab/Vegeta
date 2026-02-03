#!/bin/bash

ARR=(0 1 2 3 10 11 12 13 20 21 22 23 30 31 32 40 41 42 50 51 52 60 61 62 70 71 72 80 81 82 90 91 92)
SUM=33

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
