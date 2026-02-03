# ACS

## Getting Started

### Build
```bash
# Download dependencies.
cd src/acs
go get -v -t -d ./...

# Build ACS.
cd src/acs/server/cmd
go build -o main main.go
```

### Generate BLS keys
```bash
# Download dependencies.
cd src/crypto
go get -v -t -d ./...

# Generate bls keys.
# Default n=4, t=2
cd src/crypto/cmd/bls
go run main.go -n 4 -t 2
```

### Client
```bash
# Compile protobuf.
cd src/client/proto
make

# Build Client.
cd src/client
go build -o client main.go
```

## Testing

**First of all, you are supposed to change `key` in every shell script to the path of your own ssh key to access your AWS account**

### Run ACS on a cluster of AWS EC2s
1. You can refer to [narwhal](https://github.com/facebookresearch/narwhal) to create a cluster of EC2 machines.

2. Fetch machine information from AWS.
    ```bash
    cd script/server
    python aws.py
    ```

3. Generate confige file(`node.json`) for every node.
    ```bash
    python generate.py
    ```

4. Deliver nodes.
    ```bash
    chmod +x *.sh

    # Compress BLS keys.
    ./tarKeys.sh
    
    # Deliver to every node.
    # n is the number of nodes in the cluster.
    ./deliverNode.sh n
    ```

5. Run nodes.
   ```bash
   ./beginNode.sh n
   ```

6. Stop nodes.
   ```bash
   ./stopNode.sh n
   ```

### Run clients to send requests to ACS nodes.
1. Deliver client.
   ```bash
   cd script/client
   cp ../../src/client/client client
   
   chmod +x client
   chmod +x *.sh
   
   # n is the number of nodes in the cluster
   ./deliverNode.sh n
   ```

2. Run client for a period of time.
   ```bash
   ./beginNode.sh -payload <size of payload> -batch <size of batch> -time <running time>
   ```

3. Copy result from client node.
   ```bash
   ./createDir.sh n
   ./copyResult.sh n <name of log file>
   ```

4. Calculate throughput and latency.
   ```bash
   python cal.py <number of nodes> <batchsize> <path of log directory> <name of log file>
   ```

## Brief introduction of scripts

### script/server

* aws.py: get machine information from AWS.
  > You may need to change line 5-7 and 23 to your own config.

* generate.py：generate configuration for every node.

* tarKeys.sh: compress BLS keys.

* deliverNode.sh: deliver node to remote machines.
  * `./deliverNode.sh <the number of remote machines>`

* beginNode.sh: run node on remote machines.
  * `./beginNode.sh <the number of remote machines>`

* stopNode.sh: stop node on remote machines.
  * `./stopNode.sh <the number of remote machines>`

* Simulate the crash of specific nodes.
  * You can refer to `crash33.sh`, and change `ADDR` to your specific situation.
  * `./crash33.sh`

* Simulate the crash of the last few nodes.
  * eg. simulate the crash of the last 33 nodes of 100 nodes.
    ```bash
    ./crash.sh 100 33
    ```

* rmLog.sh: remove log file on remote machines.
  * `./rmLog.sh <the number of remote machines>`

### script/client
* deliverNode.sh: deliver client to remote machines.
  * `./deliverNode.sh <the number of remote machines>`

* Get log files from remote machines
    ```bash
    mkdir log
    ./createDir.sh <the number of remote machines>
    ./copyResult.sh <the number of remote machines> <name of log files>
    ```

* beginNode.sh: run client on remote machines.
  * These args are supposed to be assigned.
    * -payload: the size of a single request
    * -time: running time
    * -batch: the size of batch
