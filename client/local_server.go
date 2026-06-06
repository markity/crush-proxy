package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"

	"crush-proxy/comm"
)

var ConnCnt atomic.Int64

// 处理 browser 和 local server 建立的 tcp 连接
func HandleHttpProxyConn(conn *net.TCPConn, scheduler *ScheduleMaintainer) {
	// DEBUG: 计数器
	if Config.Debug {
		ConnCnt.Add(1)
		defer func() {
			ConnCnt.Add(-1)
		}()
	}
	defer func() {
		conn.Close()
	}()

	br := bufio.NewReader(conn)
	for {
		// ReadRequest only support http 1.x
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}

		// TODO: 此处可以考虑实现客户端分流, 即为部分流量不走代理
		DebugLogger.Printf("local server got a %v request, url: %v, submiting", req.Method, req.URL.String())

		task, err := scheduler.SubmitTask(context.Background(), req, conn, br)
		if err != nil {
			DebugLogger.Printf("submit task failed: %v", err)
			_, _ = conn.Write([]byte(comm.BadGatewayString))
			break
		}

		shouldCloseForce := HandleSingleTaskResult(conn, task)
		DebugLogger.Printf("a request task is handled and shoudCloseForce: %v", shouldCloseForce)
		if shouldCloseForce {
			break
		}

		if req.Header.Get("Keep-Alive") == "true" {
			DebugLogger.Println("keep-alive between browser and local proxy")
			continue
		}
		break
	}
}

func HandleSingleTaskResult(browserConn *net.TCPConn, task *Task) (shouldForceClose bool) {
	var stopCause error
	defer func() {
		if stopCause != nil || task.Request.Method == http.MethodConnect {
			shouldForceClose = true
		}
		if stopCause != nil {
			task.Stop(stopCause)
		}
	}()

	fetcher := task.GetFetcher()
	DebugLogger.Println("waiting task's header")

	// 先拿response header
	headerResp, err := fetcher.WaitHeader()
	if err != nil {
		_, err := browserConn.Write([]byte(comm.BadGatewayString))
		if err != nil {
			stopCause = fmt.Errorf("failed to write to browser: %w", err)
			return
		}
		// header都等不到，返回badgateway完成任务
		stopCause = nil
		return
	}

	// TODO: 这边可以修改response信息，比如改keep-alive

	responseHeaderBytes, err := comm.BuildRawResponseHeader(headerResp)
	if err != nil {
		stopCause = fmt.Errorf("failed to parse response body: %w", err)
		return
	}
	// 如果是connect直接用这个
	if headerResp.Request.Method == http.MethodConnect {
		responseHeaderBytes = []byte(comm.EstablishedString)
	}

	_, err = browserConn.Write(responseHeaderBytes)
	if err != nil {
		stopCause = fmt.Errorf("failed to write to browser: %w", err)
		return
	}
	DebugLogger.Printf("write response header to browser: %s\n", string(responseHeaderBytes))

	// 中间的data
	for {
		bodyFrame, err := fetcher.WaitNextBody()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			stopCause = fmt.Errorf("get body data failed: %w", err)
			return
		}

		n, err := browserConn.Write(bodyFrame)
		if err != nil {
			stopCause = fmt.Errorf("failed to write to browser: %w", err)
			return
		}

		DebugLogger.Printf("write body data to browser: %v\n", n)
	}

	<-task.TaskDone

	DebugLogger.Println("a task finished")
	return false
}

func RunHttpProxyServer(scheduler *ScheduleMaintainer) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP(Config.LocalHost), Port: Config.LocalPort})
	if err != nil {
		panic(err)
	}

	for {
		DebugLogger.Printf("new browser conn connected\n")
		tcpConn, err := listener.AcceptTCP()
		if err != nil {
			continue
		}

		go HandleHttpProxyConn(tcpConn, scheduler)
	}
}
