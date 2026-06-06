package main

import (
	"bufio"
	"context"
	"crush-proxy/comm"
	"crush-proxy/packet"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	_ "net/http/pprof"

	"golang.org/x/net/quic"
)

func perf() {
	http.ListenAndServe("localhost:6060", nil)
}

// crush-server-server ./server.conf
func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s server.conf", os.Args[0])
	}

	LoadConfig(os.Args[1])

	if Config.Debug {
		go func() {
			for {
				time.Sleep(time.Second)
				DebugLogger.Printf("current stream cnt is %v\n", StreamCnt.Load())
			}
		}()
		go perf()
	}

	DebugLogger.Printf("Server will listen on %s:%d", Config.ListenHost, Config.ListenPort)

	cert, err := tls.LoadX509KeyPair(Config.Cert, Config.Key)
	if err != nil {
		log.Fatalf("failed to load certificate: %v", err)
	}

	endpointServer, err := quic.Listen(
		"udp",
		fmt.Sprintf("%s:%d", Config.ListenHost, Config.ListenPort),
		&quic.Config{
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS13,
				NextProtos:   []string{"crush-proxy"},
			},
		},
	)
	if err != nil {
		log.Fatalf("failed to create TLS listener: %v", err)
	}
	defer endpointServer.Close(context.Background())

	for {
		conn, err := endpointServer.Accept(context.Background())
		if err != nil {
			DebugLogger.Printf("failed to accept QUIC connection: %v", err)
			continue
		}

		go handleQUICConnection(conn)
	}
}

func heartbeatHandshake(ctx context.Context, conn *quic.Conn) (*quic.Stream, error) {
	DebugLogger.Println("doing accept stream")
	mainStream, err := conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	DebugLogger.Println("main stream accepted")

	mainStream.SetReadContext(ctx)
	mainStream.SetWriteContext(ctx)
	// 2字节msgType, 4字节长度 后面是data，心跳包的data是{}
	heartbeat := make([]byte, len(packet.HeartbeatBytes))
	var done1 = make(chan error, 1)
	var done2 = make(chan error, 1)
	go func() {
		var errChan error
		defer func() {
			done1 <- errChan
		}()
		_, err := io.ReadFull(mainStream, heartbeat)
		if err != nil {
			errChan = fmt.Errorf("failed to recv heartbeat: %w", err)
			return
		}

		DebugLogger.Println("handshake heartbeat recv")
	}()
	go func() {
		var errChan error
		defer func() {
			done2 <- errChan
		}()
		_, err := mainStream.Write(packet.HeartbeatBytes)
		if err != nil {
			errChan = fmt.Errorf("failed to send heartbeat: %w", err)
		}
		mainStream.Flush()
		DebugLogger.Println("handshake heartbeat sent")
	}()

	var err1 = <-done1
	var err2 = <-done2
	if err1 != nil || err2 != nil {
		mainStream.Close()
	}
	if err1 != nil {
		return nil, fmt.Errorf("protocol error when handshake: %w", err1)
	}
	if err2 != nil {
		return nil, fmt.Errorf("protocol error when handshake: %w", err2)
	}
	DebugLogger.Println("send and recv all done")

	mainStream.SetWriteContext(context.Background())
	mainStream.SetReadContext(context.Background())

	protocolHandshakeErr := fmt.Errorf("protocol error when handshake: heartbeat received not valid")

	msgType := binary.BigEndian.Uint16(heartbeat[0:2])
	dataLen := binary.BigEndian.Uint32(heartbeat[2:6])
	if msgType != packet.Heartbeat || dataLen != uint32(len(packet.HeartbeatBytes)-6) {
		return nil, protocolHandshakeErr
	}
	pkt, err := packet.MakePacketFromJson(packet.PacketType(msgType), heartbeat[6:])
	if err != nil {
		return nil, protocolHandshakeErr
	}
	if pkt.PackType != packet.Heartbeat {
		return nil, protocolHandshakeErr
	}

	DebugLogger.Println("heartbeat is valid, main stream create success")

	return mainStream, nil
}

func handleQUICConnection(conn *quic.Conn) {
	DebugLogger.Println("new conn")
	defer conn.Close()

	handshakeCtx, handshakeCancel := context.WithDeadline(context.Background(),
		time.Now().Add(time.Millisecond*time.Duration(Config.HeartbeatTimeoutMs)))
	mainStream, err := heartbeatHandshake(handshakeCtx, conn)
	if err != nil {
		handshakeCancel()
		return
	}
	handshakeCancel()
	fmt.Println("handshake ok")

	// writer和reader will send error when exit
	writerExitChan := make(chan error, 1)
	readerExitChan := make(chan error, 1)
	accepterExitChan := make(chan error, 1)

	inChan := make(chan *packet.Packet, 512)
	outChan := make(chan []byte, 512)

	// 用来管理writer和reader, 以及accepter的生命周期, 简单来说它就是quic连接的生命周期
	ctx, cancel := context.WithCancelCause(context.Background())
	mainStream.SetReadContext(ctx)
	mainStream.SetWriteContext(ctx)

	// writer
	go func() {
		var errOut error = nil
		defer func() {
			writerExitChan <- errOut
		}()

		for {
			select {
			case bytes := <-outChan:
				_, err = comm.MakeFlushReaderWriter(mainStream).Write(bytes)
				if err != nil {
					errOut = err
					return
				}
			case <-ctx.Done():
				errOut = ctx.Err()
				return
			}
		}
	}()

	// reader, and parse packet
	go func() {
		var errOut error
		defer func() {
			readerExitChan <- errOut
		}()

		header := make([]byte, 6)
		for {
			if _, err := io.ReadFull(mainStream, header); err != nil {
				errOut = err
				return
			}

			msgType := binary.BigEndian.Uint16(header[0:2])
			bodyLen := binary.BigEndian.Uint32(header[2:6])

			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(mainStream, body); err != nil {
				errOut = err
				return
			}

			packet, err := packet.MakePacketFromJson(packet.PacketType(msgType), body)
			if err != nil {
				errOut = err
				return
			}

			inChan <- packet
		}
	}()

	// accepter
	go func() {
		var errOut error = nil
		defer func() {
			accepterExitChan <- errOut
		}()
		for {
			newStream, err := conn.AcceptStream(ctx)
			if err != nil {
				errOut = err
				return
			}

			// 处理单条stream
			DebugLogger.Printf("go handleSingleStream")
			// stream属于quic连接, 所以也共享生命周期
			go handleSingleStream(ctx, newStream)
		}
	}()

	ticker := time.NewTicker(time.Second)
	tickLoss := 0
	connClosed := false
	var stopCause error

	cleanResource := func(err error) {
		ticker.Stop()
		cancel(err)
		_, _ = <-writerExitChan
		_, _ = <-readerExitChan
		connClosed = true
	}

	// 主要控制器
	for !connClosed {
		select {
		case <-ticker.C:
			// 发心跳包
			go func() {
				select {
				case outChan <- packet.HeartbeatBytes:
				case <-ctx.Done():
					return
				}
			}()

			// 心跳检测
			tickLoss++
			if tickLoss >= 3 {
				connClosed = true
				stopCause = errors.New("heartbeat lost")
			}
		case _ = <-inChan:
			tickLoss = 0
		case err := <-writerExitChan:
			connClosed = true
			stopCause = err
		case err := <-readerExitChan:
			connClosed = true
			stopCause = err
		}
	}
	cleanResource(stopCause)
}

var StreamCnt atomic.Int64

func handleSingleStream(ctx context.Context, stream *quic.Stream) {
	DebugLogger.Println("handle single stream")
	if Config.Debug {
		StreamCnt.Add(1)
		defer func() {
			StreamCnt.Add(-1)
		}()
	}
	defer func() {
		stream.Close()
	}()

	controlCtx, controlCtxCancel := context.WithCancel(ctx)
	defer controlCtxCancel()

	stream.SetReadContext(controlCtx)
	stream.SetWriteContext(controlCtx)

	bufReader := bufio.NewReader(stream)
	clientReq, err := http.ReadRequest(bufReader)
	if err != nil {
		DebugLogger.Println(err)
		return
	}
	defer clientReq.Body.Close()

	// TODO: 这里可以做一些安全策略，检查url

	isConnect := false

	switch clientReq.Method {
	case http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPost,
		http.MethodHead, http.MethodOptions, http.MethodPatch:
	// connect 和 trace不支持
	case http.MethodConnect:
		isConnect = true
	default:
		return
	}

	if !isConnect {
		targetURL := "http://" + clientReq.Host + clientReq.URL.RequestURI()
		DebugLogger.Printf("doing http request: %v %v\n", clientReq.Method, targetURL)
		targetRequest, err := http.NewRequest(clientReq.Method, targetURL, clientReq.Body)
		if err != nil {
			DebugLogger.Println(err)
			return
		}
		for k, v := range clientReq.Header {
			targetRequest.Header[k] = v
		}

		// read body, do request 都会被controlCtx控制
		targetRequest = targetRequest.WithContext(controlCtx)

		targetResponse, err := http.DefaultClient.Do(targetRequest)
		if err != nil {
			DebugLogger.Println(err)
			return
		}
		defer targetResponse.Body.Close()
		fmt.Printf("targetResponse header: %v\n", targetResponse.Header)

		responseHeaderBytes, err := comm.BuildRawResponseHeader(targetResponse)
		if err != nil {
			DebugLogger.Println(err)
			return
		}

		DebugLogger.Printf("response get, then writing header: %s\n", string(responseHeaderBytes))
		if _, err := stream.Write(responseHeaderBytes); err != nil {
			DebugLogger.Println(err)
			return
		}

		DebugLogger.Printf("response get, then writing body\n")

		// 这里担心targetResponse.Body的读永久阻塞
		n, err := io.Copy(comm.MakeFlushReaderWriter(stream), targetResponse.Body)
		fmt.Printf("write response length %v, %v\n", n, err)
	} else {
		DebugLogger.Println("it is a connect request")
		remoteConn, err := net.Dial("tcp", clientReq.Host)
		if err != nil {
			DebugLogger.Println(err)
			return
		}
		stream.Write([]byte(comm.EstablishedString))
		stream.Flush()

		go func() {
			<-controlCtx.Done()
			remoteConn.Close()
		}()

		DebugLogger.Printf("dail %v ok, connected\n", clientReq.Host)

		outChan1 := make(chan error, 1)
		outChan2 := make(chan error, 1)
		go func() {
			_, err := io.Copy(comm.MakeFlushReaderWriter(stream), remoteConn)
			stream.CloseWrite()
			stream.Flush()
			outChan1 <- err
		}()

		go func() {
			_, err := io.Copy(remoteConn, stream)
			outChan2 <- err
		}()

		select {
		case err := <-outChan1:
			if err != nil {
				DebugLogger.Printf("handle single stream copy remote website to proxy stream err: %v", err)
			}
			controlCtxCancel()
			remoteConn.Close()
			stream.Close()
			<-outChan2
		case err := <-outChan2:
			if err != nil {
				DebugLogger.Printf("handle single stream copy proxy stream to remote website err: %v", err)
			}
			controlCtxCancel()
			remoteConn.Close()
			stream.Close()
			<-outChan1
		}
	}
}
