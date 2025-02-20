package main

import (
	// "bufio"
	// "bytes"
	"fmt"
	"sync/atomic"

	// "io"
	"flag"
	// "path/filepath"
	"strconv"
	"strings"
	"time"

	// "encoding/binary"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"os"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"

	// "github.com/ethereum/go-ethereum/ethLogger"
	"github.com/ethereum/go-ethereum/ethdb"
)

const CONFIG_FILE = "node.json"

var (
	upNum       = 16774645
	endNum      = 16774745
	dbPath      = "/home/ubuntu/ethdata/geth/chaindata"
	ancientPath = dbPath + "/ancient/chain"
	// upNum       = 17034770
	// endNum      = 17034870
	// dbPath      = "/home/user/eth/eth_data/geth/chaindata"
	// ancientPath = dbPath + "/ancient/chain"
	currState *state.StateDB
	pendingN  int32
)

// type Configuration struct {
// 	Id      uint64 `json:"id"`
// 	Port    int    `json:"port"`
// 	Key     string `json:"key_path"`
// 	Cluster string `json:"cluster"`
// }

func main() {
	numOfNodes := flag.Int("num", 1, "the number of nodes")
	execPort := flag.Int("execPort", 8000, "the port of execlayer")
	id := flag.Int("id", 0, "id of the node")
	flag.Parse()

	// var path string
	// if ex, err := os.Executable(); err == nil {
	// 	path = filepath.Dir(ex)
	// }
	// jsonFile, err := os.Open(CONFIG_FILE)
	// if err != nil {
	// 	panic(fmt.Sprint("os.Open: ", err))
	// }
	// defer jsonFile.Close()

	// data, err := ioutil.ReadAll(jsonFile)
	// if err != nil {
	// 	panic(fmt.Sprint("ioutil.ReadAll: ", err))
	// }
	// var config Configuration
	// json.Unmarshal([]byte(data), &config)
	// id := config.Id
	url := "http://127.0.0.1:" + strconv.Itoa(*execPort) + "/execlayer"

	ancientDB, err := rawdb.NewLevelDBDatabaseWithFreezer(dbPath, 16, 1, ancientPath, "", true)
	if err != nil {
		panic(err)
	}
	bc, _ := core.NewBlockChain(ancientDB, nil, core.DefaultGenesisBlock().Config, ethash.NewFaker(), vm.Config{}, nil, nil)
	stateDB := buildStateDB(ancientDB, upNum-1)
	currState = stateDB.Copy()

	// //conn, err := net.Dial("tcp", "127.0.0.1:8001")
	// tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8001")
	// connTcp, err := net.DialTCP("tcp", nil, tcpAddr)
	// connTcp.SetWriteBuffer(64 * 1024)
	// connTcp.SetReadBuffer(64 * 1024)
	// if err != nil {
	// 	fmt.Println("conn dial err = ", err)
	// }
	// go handleSpeculation(ancientDB, bc, connTcp, id)

	file, err := os.OpenFile("execlayer.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reqC := make(chan []byte, 100)
	respC := make(chan []*core.ConsensusContent, 100)
	waitC := make(chan bool, 1)
	serialTime := new(time.Duration)
	parallelTime := new(time.Duration)
	startTime := time.Now()
	endTime := time.Now()
	execTime := new(time.Duration)
	//oldTime := endTime
	go processSpeculation(ancientDB, bc, *id, *numOfNodes, reqC, waitC)
	go sendRequest(url, reqC, respC)

	// respC := make(chan []*core.ConsensusContent, 100)
	// go handleResp(connTcp, respC)

	for {
		resp, ok := <-respC
		if !ok {
			break
		}
		var processN int
		// stateDB = buildStateDB(ancientDB, upNum-1)
		execStart := time.Now()
		for _, cc := range resp {
			fmt.Println("Block number: ", cc.BlockNum)
			processReplay(bc, stateDB, cc, serialTime, parallelTime)
			currState.StateCopy(stateDB)
			processN = int(cc.BlockNum.Int64())
			if atomic.AddInt32(&pendingN, -1) == 2 {
				waitC <- true
			}
			if (processN-upNum)%500 == 0 {
				endTime = time.Now()
				stateDB = buildStateDB(ancientDB, upNum-1)
				file.Write([]byte(fmt.Sprintf("block number: %+v\n", processN)))
				file.Write([]byte(fmt.Sprintf("serial execution time: %+v\n", serialTime)))
				file.Write([]byte(fmt.Sprintf("parallel execution time: %+v\n", parallelTime)))
				file.Write([]byte(fmt.Sprintf("other time: %+v\n\n", endTime.Sub(startTime)-*execTime)))
			}
		}
		currState = stateDB.Copy()
		*execTime += time.Since(execStart)
		//endTime = time.Now()
		//file.Write([]byte(fmt.Sprintf("time of executing one batch: %+v\n", endTime.Sub(oldTime))))
		//oldTime = endTime
		if processN == endNum+*numOfNodes-1 {
			break
		}
	}
	endTime = time.Now()

	// file.Write([]byte("start time: " + startTime.String() + "\n"))
	// file.Write([]byte("end time: " + endTime.String() + "\n"))
	file.Write([]byte(fmt.Sprintf("serial execution time: %+v\n", serialTime)))
	file.Write([]byte(fmt.Sprintf("parallel execution time: %+v\n", parallelTime)))
	file.Write([]byte(fmt.Sprintf("other time: %+v\n", endTime.Sub(startTime)-*execTime)))
}

func buildStateDB(ancientDB ethdb.Database, blockNum int) *state.StateDB {
	blkHash := rawdb.ReadCanonicalHash(ancientDB, uint64(blockNum))
	block := rawdb.ReadBlock(ancientDB, blkHash, uint64(blockNum))
	sdb := state.NewDatabase(ancientDB)
	statedb, err := state.New(block.Root(), sdb, nil)
	if err != nil {
		panic(err)
	}

	return statedb
}

// func handleSpeculation(ancientDB ethdb.Database, bc *core.BlockChain, conn net.Conn, id uint64) {
// 	var txSum int
// 	var cc *core.ConsensusContent
// 	for i := upNum; i <= upNum; i = i + 10 {
// 		preStateDB := buildStateDB(ancientDB, i-1)
// 		blkHash := rawdb.ReadCanonicalHash(ancientDB, uint64(i)+id)
// 		block := rawdb.ReadBlock(ancientDB, blkHash, uint64(i)+id)
// 		txSum += len(block.Transactions())
// 		cc = bc.Processor().Speculate(block, preStateDB, vm.Config{})

// 		data, err := json.Marshal(cc)
// 		fmt.Printf("data: %v\n", string(data))
// 		if err != nil {
// 			fmt.Println("json.Marshal error")
// 			return
// 		}
// 		msg, _ := Pack(data)
// 		fmt.Printf("msg: %v\n", string(msg))
// 		_, err = conn.Write(msg)
// 		if err != nil {
// 			fmt.Println("conn write err = ", err)
// 		}
// 	}
// }

// func handleResp(conn net.Conn, respC chan []*core.ConsensusContent) {
// 	defer close(respC)

// 	var resp []*core.ConsensusContent
// 	reader := bufio.NewReader(conn)
// 	for {
// 		data, err := Unpack(reader)
// 		if err == io.EOF {
// 			return
// 		} else if err != nil {
// 			fmt.Printf("read response err = %v\n", err)
// 			return
// 		}
// 		err = json.Unmarshal(data, &resp)
// 		if err != nil {
// 			fmt.Printf("json unmarshal err = %v\n", err)
// 		}
// 		fmt.Printf("resp: %v\n", resp)
// 		respC <- resp
// 	}
// }

func processSpeculation(ancientDB ethdb.Database, bc *core.BlockChain, id int, n int, reqC chan []byte, waitC chan bool) {
	for i := upNum; i <= endNum; i = i + n {
		preStateDB := currState.Copy()
		blkHash := rawdb.ReadCanonicalHash(ancientDB, uint64(i+id))
		block := rawdb.ReadBlock(ancientDB, blkHash, uint64(i+id))
		cc := bc.Processor().Speculate(block, preStateDB, vm.Config{})
		// b, _ := block.EncodeToBytes()
		// cc := &core.ConsensusContent{
		// 	BlockNum: block.Number(),
		// 	BytesOfBlock: b,
		// }
		data, err := json.Marshal(cc)
		//fmt.Printf("data: %v\n", string(data[0]))
		if err != nil {
			fmt.Println("json.Marshal error")
			return
		}
		reqC <- data
		atomic.AddInt32(&pendingN, int32(n))
		<-waitC
	}
	endFlag := "end"
	reqC <- []byte(endFlag)
}

func processReplay(bc *core.BlockChain, stateDB *state.StateDB, cc *core.ConsensusContent, serialTime *time.Duration, parallelTime *time.Duration) {
	loadTime := new(time.Duration)
	bc.Processor().Serial(cc, stateDB, vm.Config{}, loadTime)

	bc.Processor().Serial(cc, stateDB, vm.Config{}, serialTime)
	bc.Processor().Parallel(cc, stateDB, vm.Config{}, parallelTime)
	//bc.Processor().DeOcc(cc, stateDB, vm.Config{}, occTime)
}

// func Pack(data []byte) ([]byte, error) {
// 	length := uint64(len(data))
// 	pkg := new(bytes.Buffer)

// 	err := binary.Write(pkg, binary.LittleEndian, length)
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = binary.Write(pkg, binary.LittleEndian, data)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return pkg.Bytes(), nil
// }

// func Unpack(reader *bufio.Reader) ([]byte, error) {
// 	lengthByte, _ := reader.Peek(8)
// 	lengthBuff := bytes.NewBuffer(lengthByte)
// 	var length uint64
// 	err := binary.Read(lengthBuff, binary.LittleEndian, &length)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if uint64(reader.Buffered()) < length+8 {
// 		return nil, err
// 	}

// 	pack := make([]byte, int(length+8))
// 	_, err = reader.Read(pack)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return pack[8:], nil
// }

func sendRequest(url string, reqC chan []byte, respC chan []*core.ConsensusContent) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     time.Duration(60),
		},
	}
	for {
		data := <-reqC
		if string(data) == "end" {
			break
		}
		body := strings.NewReader(string(data))
		req, err := http.NewRequest("POST", url, body)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		respData, err := ioutil.ReadAll(resp.Body)
		var ccList []*core.ConsensusContent
		json.Unmarshal(respData, &ccList)
		respC <- ccList
	}
}
