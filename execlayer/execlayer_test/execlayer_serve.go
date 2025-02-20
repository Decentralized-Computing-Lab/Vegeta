package main

import (
	"fmt"
	// "github.com/ethereum/go-ethereum/core"
	"io/ioutil"
	"net/http"
	"strconv"
)

func main() {
	// listenSpec, err := net.Listen("tcp", "127.0.0.1:8001")
	// if err != nil {
	// 	fmt.Println("listen err = ", err)
	// 	return
	// }
	// defer listenSpec.Close()

	// connSpec, err := listenSpec.Accept()
	// if err != nil {
	// 	fmt.Println("Accept() err = ", err)
	// }
	// defer connSpec.Close()

	reqC := make(chan []byte, 100)
	respC := make(chan []byte, 100)
	// go processSpec(connSpec, dataC)

	rprocessor := &ExecLayerMsgProcessor{reqC: reqC, respC: respC}
	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)
	mux.Handle("/execlayer", rprocessor)
	mux.Handle("/execlayer/", rprocessor)
	server := &http.Server{Addr: ":" + strconv.Itoa(8001), Handler: mux}
	server.SetKeepAlivesEnabled(true)

	go server.ListenAndServe()

	var resp string
	for {
		req, ok := <-reqC
		if !ok {
			break
		}
		resp = "[" + string(req) + "]"
		respC <- []byte(resp)
	}
	// resp = "[" + resp[:len(resp)-1] + "]"
	// fmt.Println(resp)
	// respC <- []byte(resp)
	// msg, _ := Pack([]byte(resp))
	// fmt.Printf("resp: %v\n", string(msg))
	// connSpec.Write(msg)
}

type ExecLayerMsgProcessor struct {
	reqC  chan []byte
	respC chan []byte
}

func (emsgProcessor *ExecLayerMsgProcessor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed on PUT", http.StatusBadRequest)
		fmt.Println("Failed on PUT", http.StatusBadRequest)
		return
	}
	if len(v) == 0 {
		fmt.Println("error request size")
		v = make([]byte, 4)
	}
	fmt.Println(string(v[0]))
	emsgProcessor.reqC <- v
	resp := <-emsgProcessor.respC
	w.Write(resp)
}

// func processSpec(conn net.Conn, dataC chan []byte) {
// 	// defer conn.Close()
// 	defer close(dataC)

// 	reader := bufio.NewReader(conn)
// 	for {
// 		data, err := Unpack(reader)
// 		if err == io.EOF {
// 			return
// 		}
// 		if err != nil {
// 			fmt.Printf("read response err = %v\n", err)
// 			return
// 		}
// 		fmt.Printf("data: %v\n", string(data))
// 		dataC <- data
// 	}

// }

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
// 	fmt.Println(length)

// 	if uint64(reader.Buffered()) < length+8 {
// 		//fmt.Printf("reader buffered: %v\n", reader.Buffered())
// 		return nil, err
// 	}

// 	pack := make([]byte, int(length+8))
// 	_, err = reader.Read(pack)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return pack[8:], nil
// }
