#!/bin/bash

NUM=$1

for ((i = 0; i < NUM; i++)); do
{
        host1=$(jq ".nodes[$i].PublicIpAddress" nodes.json)
        host=${host1//\"/}
        port=5000
        user='ubuntu'
        key="~/.ssh/eth_parallel.pem"
        id=$i
        node="node"$id

        expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "sudo firewall-cmd --zone=public --add-port=6000/tcp --permanent;sudo firewall-cmd --zone=public --add-port=8000/tcp --permanent;sudo firewall-cmd --reload;cd;mkdir ACSserver;mkdir -p ACSserver/conf;mkdir -p ACSserver/script;mkdir -p ACSserver/crypto;mkdir -p ACSserver/log;cd ACSserver/log;touch server$id"
expect EOF
exit
END

        expect -c "
set timeout -1
spawn scp -i $key ../../src/acs/server/cmd/ACSserver  $user@$host:ACSserver/
expect 100%
exit
"

        expect -c "
set timeout -1
spawn scp -i $key crypto.tar.gz $user@$host:ACSserver/crypto.tar.gz
expect 100%
exit
"

        expect -c "
set timeout -1
spawn scp -i $key stop.sh $user@$host:ACSserver/script/
expect 100%
exit
"

        expect -c "
set timeout -1
spawn scp -i $key $node.json $user@$host:ACSserver/
expect 100%
exit
"

        expect <<-END
spawn ssh -oStrictHostKeyChecking=no -i $key $user@$host "cd;chmod 777 ACSserver/ACSserver;cd ACSserver/script;chmod 777 stop.sh;cd ..;mv $node.json node.json;rm -rf crypto;tar -xvf crypto.tar.gz"
expect EOF
exit
END
} &
done

wait
