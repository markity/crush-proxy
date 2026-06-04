package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	"crush-proxy/comm"
)

// 处理单条http代理连接, 这里是一来一回的阻塞操作, 需要单独起一个goroutine
func HandleHttpProxyConn(conn *net.TCPConn, scheduler *ScheduleMaintainer) {
	defer conn.Close()

	connBufReader := bufio.NewReader(conn)
	for {
		// ReadRequest only support http 1.x
		req, err := http.ReadRequest(connBufReader)
		if err != nil {
			return
		}
		log.Printf("read browser request: %v\n", req.URL.String())

		// TODO: 此处可以考虑实现客户端分流, 即为部分流量不走代理
		log.Printf("local server got a %v request, url: %v", req.Method, req.URL.String())

		task := MakeTask(req, connBufReader)
		recvChan := scheduler.SubmitTask(task)

		if HandleSingleTask(conn, task, recvChan) {
			return
		}

		if req.Header.Get("Keep-Alive") == "true" {
			continue
		}
		break
	}
}

func HandleSingleTask(writer io.Writer, task *Task, recvChan <-chan *FrameData) (shouldClose bool) {
	var stopCause error
	defer func() {
		task.Stop(stopCause)
	}()

	// 先拿response header
	headerFrame, _ := <-recvChan
	if headerFrame == nil || headerFrame.IsErr {
		_, err := writer.Write([]byte(comm.BadGatewayString))
		if err != nil {
			shouldClose = true
			stopCause = fmt.Errorf("failed to write to browser: %w", err)
			return
		}
	} else {
		// TODO: 这边可以修改response信息，比如改keep-alive

		responseHeaderBytes, err := comm.BuildRawResponseHeader(headerFrame.Response)
		if err != nil {
			stopCause = fmt.Errorf("failed to parse response body: %w", err)
			return
		}

		if headerFrame.Response.Request.Method == http.MethodConnect {
			responseHeaderBytes = []byte(comm.EstablishedString)
		}

		_, err = writer.Write(responseHeaderBytes)
		if err != nil {
			shouldClose = true
			stopCause = fmt.Errorf("failed to write to browser: %w", err)
			return
		}
		log.Printf("write response header to browser: %s\n", string(responseHeaderBytes))
	}

	// 中间的data
	for frame := range recvChan {
		n, err := writer.Write(frame.Data)
		if err != nil || frame.IsErr {
			shouldClose = true
			stopCause = fmt.Errorf("failed to write to browser: %w", err)
			return
		}
		log.Printf("write body data: %v\n", n)
	}

	log.Println("a task finished")
	return false
}

func RunHttpProxyServer(scheduler *ScheduleMaintainer) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP(Config.LocalHost), Port: Config.LocalPort})
	if err != nil {
		panic(err)
	}

	for {
		log.Printf("new browser conn connected\n")
		tcpConn, err := listener.AcceptTCP()
		if err != nil {
			continue
		}

		go HandleHttpProxyConn(tcpConn, scheduler)
	}
}
