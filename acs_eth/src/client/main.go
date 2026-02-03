package main

import (
	// "bufio"
	// "bytes"
	// "encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.themix.io/client/proto/clientpb"
	"google.golang.org/protobuf/proto"
)

func main() {
	// batchSize := flag.Int("batch", 2, "batch size")
	// payloadSize := flag.Int("payload", 200, "payload size")
	// keyPath := flag.String("key", "../crypto", "path of ECDSA private key")
	url := flag.String("url", "http://127.0.0.1:6000/client", "url of client")
	// testTime := flag.Int("time", 60, "test time")
	output := flag.String("output", "client.log", "output file")
	execPort := flag.Int("execPort", 8000, "the port of execlayer")
	flag.Parse()

	// buffer, err := generatePayload(*batchSize, *payloadSize, *keyPath)
	// if err != nil {
	// 	panic(err)
	// }

	reqC := make(chan []byte, 100)
	respC := make(chan []byte, 100)
	rprocessor := &ExecLayerMsgProcessor{reqC: reqC, respC: respC}
	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)
	mux.Handle("/execlayer", rprocessor)
	mux.Handle("/execlayer/", rprocessor)
	server := &http.Server{Addr: ":" + strconv.Itoa(*execPort), Handler: mux}
	server.SetKeepAlivesEnabled(true)
	go server.ListenAndServe()

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

	file, err := os.OpenFile(*output, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// startTime := time.Now()
	// endTime := time.Now()
	// oldTime := endTime
	for {
		buffer, ok := <-reqC
		if !ok {
			break
		}
		body := strings.NewReader(string(buffer))
		req, err := http.NewRequest("POST", *url, body)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		consStart := time.Now()
		resp, err := client.Do(req)
		consEnd := time.Now()
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		//endTime = time.Now()
		file.Write([]byte(fmt.Sprintf("%d\n", consEnd.Sub(consStart).Milliseconds())))
		//oldTime = endTime

		respData, err := ioutil.ReadAll(resp.Body)
		respC <- respData
	}

	// file.Write([]byte("start time: " + startTime.String() + "\n"))
	// file.Write([]byte("end time: " + endTime.String() + "\n"))
	// file.Write([]byte(fmt.Sprintf("%d\n", endTime.Sub(startTime).Milliseconds())))
}

func generatePayload(batchsize, payload int, key string) ([]byte, error) {
	message := &clientpb.ClientMessage{}
	for i := 0; i < payload; i++ {
		message.Payload += "a"
	}
	clientMessages := &clientpb.ClientMessages{}
	for i := 0; i < batchsize; i++ {
		clientMessages.Payload = append(clientMessages.Payload, message)
	}
	buffer, err := proto.Marshal(clientMessages)
	if err != nil {
		return nil, fmt.Errorf("proto.Marshal: %v", err)
	}
	return buffer, nil
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
