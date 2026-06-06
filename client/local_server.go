package main

import (
	"bufio"
	"context"
	"net"
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
		// TODO: 此处可以考虑实现客户端分流, 即为部分流量不走代理
		DebugLogger.Printf("local server got a request, submiting")

		task, err := scheduler.SubmitTask(context.Background(), conn, br)
		if err != nil {
			DebugLogger.Printf("submit task failed: %v", err)
			_, _ = conn.Write([]byte(comm.BadGatewayString))
			break
		}

		shouldClose, err := task.GetFetcher().WaitProcess()
		if err != nil {
			DebugLogger.Printf("task process failed: %v\n", err)
			return
		}
		if shouldClose {
			return
		}
	}
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
