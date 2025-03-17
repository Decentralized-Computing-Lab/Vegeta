# Vegeta

## single node

1. create an instance from aws ami "eth_parallel"(instance type: m6i.4xlarge)
2. connect to the instance and initialize EBS volume:
```bash=
sudo apt-get install -y fio
sudo fio --filename=/dev/sda1 --rw=read --bs=1M --iodepth=32 --ioengine=libaio --direct=1 --name=volume-initialize
```

3. open folder `eth/eth_parallel/ethInfo`
4. test:
```bash=
go run newEthLeveldb.go
```
    
or run in the backstage:
```bash=
nohup go run newEthLeveldb.go > log 2>&1 &
```

## multi nodes
### initialization

#### 1. create an instance from aws ami "eth_parallel"

connect to the instance and initialize EBS volume:
```bash=
sudo apt-get install -y fio
sudo fio --filename=/dev/nvme0n1 --rw=read --bs=1M --iodepth=32 --ioengine=libaio --direct=1 --name=volume-initialize
```

open the firewall:
```bash=
sudo firewall-cmd --zone=public --add-port=6000/tcp --permanent
sudo firewall-cmd --reload
```

#### 2. ACSserver
```bash=
# Download dependencies.
cd acs_eth/src/acs
go get -v -t -d ./...

# Build ACSserver.
cd acs_eth/src/acs/server/cmd
go build -o ACSserver main.go
```

#### 3. BLS keys
```bash=
# Download dependencies.
cd acs_eth/src/crypto
go get -v -t -d ./...

# Generate bls keys.
# Default n=4, t=2
cd acs_eth/src/crypto/cmd/bls
go run main.go -n 4 -t 2
```

#### 4. ACSclient
```bash=
# Compile protobuf.
cd acs_eth/src/client/proto
make

# Build Client.
cd acs_eth/src/client
go build -o ACSclient main.go
```

#### 5. execlayer
```bash=
# Build execlayer.
cd eth/eth_parallel/execlayer
go build -o execlayer execlayer.go
```

### test

#### 1. run ACSserver
1. fetch the information of AWS instances:
    ```bash=
    cd acs_eth/script/server
    python3 aws.py
    ```

3. generate configuration file `node.json`
    ```bash=
    python3 generate.py
    ```

4. deliver ACSserver and keys
    ```bash=
    chmod +x *.sh

    # Compress BLS keys.
    ./tarKeys.sh
    
    # Deliver to every node.
    # n is the number of nodes in the cluster.
    ./deliverNode.sh n
    ```

5. run
   ```bash=
   ./beginNode.sh n
   ```

6. stop all the programs
   ```bash=
   ./stopNode.sh n
   ```

#### 2. run ACSclient
1. deliver ACSclient
   ```bash=
   cd acs_eth/script/client
   cp ../../src/client/ACSclient ACSclient
   
   chmod +x ACSclient
   chmod +x *.sh
   
   # n is the number of nodes in the cluster
   ./deliverNode.sh n
   ```

2. run
   ```bash=
   ./beginNode.sh n
   ```
   

#### 3. run execlayer
1. deliver execlayer
   ```bash=
   cd acs_eth/script/execlayer
   cp ../../../eth/eth_parallel/execlayer/execlayer execlayer
   
   chmod +x execlayer
   chmod +x *.sh
   
   # n is the number of nodes in the cluster
   ./deliverNode.sh n
   ```

2. run
   ```bash=
   ./beginNode.sh n
   ```
   
3. stop execlayer
   ```bash=
   ./stopNode.sh n
   ```