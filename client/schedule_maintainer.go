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
	"sync/atomic"
	"time"

	"golang.org/x/net/quic"
)

type ControlConnContext struct {
	UdpConn    *net.UDPConn
	Endpoint   *quic.Endpoint
	Conn       *quic.Conn
	MainStream *quic.Stream
}

// blocking operation
func (ctx *ControlConnContext) Close(mctx context.Context) {
	if ctx.MainStream != nil {
		ctx.MainStream.Close()
	}
	if ctx.Conn != nil {
		ctx.Conn.Close()
	}
	if ctx.Endpoint != nil {
		ctx.Endpoint.Close(mctx)
	}
	if ctx.UdpConn != nil {
		ctx.UdpConn.Close()
	}
}

// 连接quic并且创建一条stream, 中间出现任何错误返回error
func MakeControlConnContext(ctx context.Context) (result *ControlConnContext, errOut error) {
	result = &ControlConnContext{}
	defer func() {
		if errOut != nil {
			result.Close(ctx)
		}
	}()

	// 客户端本地 UDP socket，端口用 0 让系统随机分配。
	var err error
	result.UdpConn, err = net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 0,
	})
	if err != nil {
		errOut = fmt.Errorf("failed to create local UDP socket: %w", err)
		return
	}

	// 第二个参数传 nil，表示这个 endpoint 不接受入站 QUIC 连接。
	result.Endpoint, err = quic.NewEndpoint(result.UdpConn, nil)
	if err != nil {
		errOut = fmt.Errorf("failed to create local QUIC endpoint: %w", err)
		return
	}

	remoteAddr := fmt.Sprintf("%s:%d", Config.RemoteHost, Config.RemotePort)

	result.Conn, err = result.Endpoint.Dial(
		ctx,
		"udp",
		remoteAddr,
		&quic.Config{
			TLSConfig: &tls.Config{
				// QUIC 必须是 TLS 1.3
				MinVersion: tls.VersionTLS13,
				NextProtos: []string{"crush-proxy"},
			},
		},
	)
	if err != nil {
		errOut = fmt.Errorf("failed to dial QUIC server: %w", err)
		return
	}

	DebugLogger.Printf("new conn success")

	// main stream是命令/心跳/鉴权的专用stream，先建立起来，并且互发一次心跳
	result.MainStream, err = result.Conn.NewStream(ctx)
	if err != nil {
		errOut = fmt.Errorf("failed to make main stream: %w", err)
		return
	}
	DebugLogger.Printf("new stream success")

	result.MainStream.SetReadContext(ctx)
	result.MainStream.SetWriteContext(ctx)
	// 2字节msgType, 4字节长度 后面是data，心跳包的data是{}
	heartbeat := make([]byte, len(packet.HeartbeatBytes))
	var done1 = make(chan error, 1)
	var done2 = make(chan error, 1)
	go func() {
		var errChan error
		defer func() {
			done1 <- errChan
		}()
		_, err := io.ReadFull(result.MainStream, heartbeat)
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
		_, err := result.MainStream.Write(packet.HeartbeatBytes)
		if err != nil {
			errChan = fmt.Errorf("failed to send heartbeat: %w", err)
		}
		result.MainStream.Flush()
		DebugLogger.Println("handshake heartbeat sent")
	}()

	var err1 = <-done1
	var err2 = <-done2
	if err1 != nil {
		errOut = fmt.Errorf("protocol error when handshake: %w", err1)
		return
	}
	if err2 != nil {
		errOut = fmt.Errorf("protocol error when handshake: %w", err2)
		return
	}
	DebugLogger.Println("send and recv all done")

	result.MainStream.SetWriteContext(context.Background())
	result.MainStream.SetReadContext(context.Background())

	protocolHandshakeErr := fmt.Errorf("protocol error when handshake: heartbeat received not valid")

	msgType := binary.BigEndian.Uint16(heartbeat[0:2])
	dataLen := binary.BigEndian.Uint32(heartbeat[2:6])
	if msgType != packet.Heartbeat || dataLen != uint32(len(packet.HeartbeatBytes)-6) {
		errOut = protocolHandshakeErr
		return
	}
	pkt, err := packet.MakePacketFromJson(packet.PacketType(msgType), heartbeat[6:])
	if err != nil {
		errOut = protocolHandshakeErr
		return
	}
	if pkt.PackType != packet.Heartbeat {
		errOut = protocolHandshakeErr
		return
	}

	DebugLogger.Println("heartbeat is valid, main stream create success")

	errOut = nil
	return
}

type Task struct {
	Conn          *net.TCPConn
	ConnBufReader *bufio.Reader

	TaskResultFetcher *TaskResultFetcher

	// Task User 控制task
	Ctx       context.Context
	CtxCancel context.CancelCauseFunc
}

// stop被调用时, conn直接被破坏, conn会被自动关闭
func (task *Task) Stop(err error) {
	task.CtxCancel(fmt.Errorf("task stopped: %w", err))
}

func (task *Task) GetFetcher() *TaskResultFetcher {
	return task.TaskResultFetcher
}

type ScheduleMaintainer struct {
	CurrentControl *ControlConnContext
	PendingTasks   chan *Task
}

// 主要事件循环，维护quic conn，main stream活性，处理其它地方submit来的请求创建新的stream做代理
func (maintainer *ScheduleMaintainer) RunForeverWithInitialCtx() {
	// main stream消息循环，task请求循环
	for {
		DebugLogger.Printf("connection main loop is running, local addr is: %v\n",
			maintainer.CurrentControl.UdpConn.LocalAddr().String())
		// writer和reader will send error when exit
		writerExitChan := make(chan error, 1)
		readerExitChan := make(chan error, 1)

		inChan := make(chan *packet.Packet, 2048)
		outChan := make(chan []byte, 2048)

		// 用来管理writer和reader
		ctx, cancel := context.WithCancelCause(context.Background())
		maintainer.CurrentControl.MainStream.SetReadContext(ctx)
		maintainer.CurrentControl.MainStream.SetWriteContext(ctx)

		// writer
		go func() {
			var errChan error = nil
			defer func() {
				writerExitChan <- errChan
				close(writerExitChan)
			}()

			for {
				select {
				case bytes := <-outChan:
					if _, err := comm.MakeFlushReaderWriter(maintainer.CurrentControl.MainStream).Write(bytes); err != nil {
						errChan = err
						return
					}
				case <-ctx.Done():
					errChan = ctx.Err()
					return
				}
			}
		}()

		// reader, and parse packet
		go func() {
			var errChan error
			defer func() {
				readerExitChan <- errChan
				close(readerExitChan)
			}()

			maintainer.CurrentControl.MainStream.SetReadContext(ctx)
			header := make([]byte, 6)
			for {
				if _, err := io.ReadFull(maintainer.CurrentControl.MainStream, header); err != nil {
					errChan = err
					return
				}

				msgType := binary.BigEndian.Uint16(header[0:2])
				bodyLen := binary.BigEndian.Uint32(header[2:6])

				body := make([]byte, bodyLen)
				if _, err := io.ReadFull(maintainer.CurrentControl.MainStream, body); err != nil {
					errChan = err
					return
				}

				pkt, err := packet.MakePacketFromJson(packet.PacketType(msgType), body)
				if err != nil {
					errChan = err
					return
				}

				inChan <- pkt
			}
		}()

		ticker := time.NewTicker(time.Second)
		tickLoss := 0

		connClosed := false
		var connCloseReason error

		cleanResource := func(ctx context.Context, err error) {
			ticker.Stop()
			cancel(err)
			_, _ = <-writerExitChan
			_, _ = <-readerExitChan
			maintainer.CurrentControl.Close(ctx)
		}

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
					DebugLogger.Printf("heartbeat loss, connection lost")
					connClosed = true
					connCloseReason = errors.New("heartbeat loss")
				}
			case _ = <-inChan:
				tickLoss = 0
			case task := <-maintainer.PendingTasks:
				// taskStream, err := maintainer.CurrentControl.Conn.NewStream(context.Background())
				// if err != nil {
				// 	task.TaskResultFetcher.ErrChan <- fmt.Errorf("new task stream failed: %w", err)
				// 	close(task.TaskResultFetcher.ErrChan)
				// 	connClosed = true
				// 	connCloseReason = fmt.Errorf("new task stream failed: %w", err)
				// 	break
				// }
				// go HandleTask(task, taskStream)
				go HandleTask(task, maintainer.CurrentControl.Conn)

			case err := <-writerExitChan:
				connClosed = true
				connCloseReason = fmt.Errorf("writer exit: %w", err)
			case err := <-readerExitChan:
				connClosed = true
				connCloseReason = fmt.Errorf("reader exit: %w", err)
			}
		}
		DebugLogger.Printf("exit main connection loop: %v\n", connCloseReason)

		// 清空资源
		cleanResourceCtx, cleanResourceCtxCancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*500))
		cleanResource(cleanResourceCtx, connCloseReason)
		cleanResourceCtxCancel()

		var err error
		for i := 0; i < Config.ReconnectMaxRetry; i++ {
			DebugLogger.Printf("connection lost, reconnecting %dst\n", i+1)
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Duration(Config.ConnectTimeoutMs)*time.Millisecond))
			maintainer.CurrentControl, err = MakeControlConnContext(ctx)
			if err == nil {
				cancel()
				DebugLogger.Printf("reconnect successfully\n")
				break
			}
			cancel()
		}
		if err != nil {
			log.Fatalf("server connection lost: %v", err)
		}
	}
}

func HandleTask(task *Task, quicConn *quic.Conn) {
	// 用户调用Stop后, controlCloseConnCtx作为子context会被触发
	//		此外 HandleTask出现错误，也会触发, 错误就是closeConnReason
	var closeConnReason error
	var req *http.Request
	controlCloseConnCtx, controlCtxCancel := context.WithCancelCause(task.Ctx)
	defer func() {
		controlCtxCancel(closeConnReason)
		task.TaskResultFetcher.ShouldClose.Store(true)
		if req != nil && req.Header.Get("Proxy-Connection") == "keep-alive" {
			task.TaskResultFetcher.ShouldClose.Store(false)
		}
		task.TaskResultFetcher.ErrChan <- closeConnReason
		close(task.TaskResultFetcher.ErrChan)
		DebugLogger.Println("task finished, and stream closed")
	}()
	// read browser conn, write to stream
	// read stream, write to browser conn
	// read/write tcp conn, even if ctx is triggered
	go func() {
		<-controlCloseConnCtx.Done()
		if controlCloseConnCtx.Err() != nil {
			task.Conn.Close()
		}
	}()

	req, err := http.ReadRequest(task.ConnBufReader)
	if err != nil {
		closeConnReason = fmt.Errorf("failed to read http header from proxy stream: %v", err)
		return
	}
	defer req.Body.Close()

	stream, err := quicConn.NewStream(context.Background())
	if err != nil {
		closeConnReason = fmt.Errorf("new task stream failed: %w", err)
		return
	}
	defer stream.Close()

	DebugLogger.Printf("handle task, task url: %v\n", req.URL.String())
	stream.SetReadContext(controlCloseConnCtx)
	stream.SetWriteContext(controlCloseConnCtx)

	writerExitChan := make(chan error, 1)
	readerExitChan := make(chan error, 1)

	// writer: read browserReq.body, write to stream
	go func() {
		var errChan error
		defer func() {
			if errChan == nil {
				DebugLogger.Printf("writer finished it's job")
			}
			writerExitChan <- errChan
			close(writerExitChan)
			stream.CloseWrite()
			stream.Flush()
		}()

		DebugLogger.Printf("write req to proxy server\n")
		err := req.Write(comm.MakeFlushReaderWriter(stream))
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				_, err := io.Copy(io.Discard, req.Body)
				req.Body.Close()
				if err != nil {
					errChan = fmt.Errorf("failed to copy local body to remote stream")
				} else {
					errChan = nil
				}
				return
			}
			errChan = err
			return
		}

		// 不断读body, 直到body读到eof，假如body不完全或者tcp意外断开，会产生error
		if req.Method == http.MethodConnect {
			// connect请求先丢弃req body, 再直接拷贝conn buf reader
			n, err := io.Copy(comm.MakeFlushReaderWriter(stream), task.ConnBufReader)
			DebugLogger.Printf("copy task conn buf to remote stream: %d %v\n", n, err)
			errChan = err
			return
		}
	}()

	// reader
	go func() {
		var errChan error
		defer func() {
			req.Body.Close()

			readerExitChan <- errChan
			close(readerExitChan)

			if errChan == nil {
				DebugLogger.Printf("reader finished it's job")
			}
		}()

		streamBufReader := bufio.NewReader(stream)
		DebugLogger.Println("waiting for stream's http response header")
		resp, err := http.ReadResponse(streamBufReader, req)
		if err != nil {
			DebugLogger.Printf("handleTask, read response header failed: %v\n", err)
			// 不会阻塞
			errChan = fmt.Errorf("failed to read response: %w", err)
			return
		}
		DebugLogger.Printf("HandleTask, got response from proxy server stream: %v\n", resp.Status)
		resp.Write(task.Conn)
		err = task.Conn.CloseWrite()
		if err != nil {
			log.Printf("failed to write response from stream to local browser conn: %v\n", err)
			closeConnReason = err
			return
		}
	}()

	select {
	case errReader := <-readerExitChan:
		if errReader != nil {
			controlCtxCancel(errReader)
			closeConnReason = fmt.Errorf("reader exit with err: %w", errReader)
			return
		}
		<-writerExitChan
	case errWriter := <-writerExitChan:
		if errWriter != nil {
			controlCtxCancel(errWriter)
			closeConnReason = fmt.Errorf("writer exit with err: %w", errWriter)
			return
		}
		// if req.Method == http.MethodConnect {
		// 	controlCtxCancel(errors.New("connect tunnel writer closed"))
		// }
		<-readerExitChan
	}

	DebugLogger.Println("HandleTask returned")
}

type TaskResultFetcher struct {
	Ctx         context.Context
	ErrChan     chan error
	ShouldClose atomic.Bool
}

// bool notifies weather the conn is useful, if false, conn is closed
func (fetcher *TaskResultFetcher) WaitProcess() (bool, error) {
	err, ok := <-fetcher.ErrChan
	if !ok {
		return false, errors.New("wait process can only be used once")
	}

	return fetcher.ShouldClose.Load(), err
}

func (fetcher *TaskResultFetcher) err() error {
	select {
	case err := <-fetcher.ErrChan:
		return err
	default:
		return nil
	}
}

func MakeScheduleMatainer(control *ControlConnContext, taskInChanSize int) *ScheduleMaintainer {
	return &ScheduleMaintainer{CurrentControl: control, PendingTasks: make(chan *Task, taskInChanSize)}
}

func makeTask(conn *net.TCPConn, connBufReader *bufio.Reader) *Task {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Task{
		Conn:          conn,
		ConnBufReader: connBufReader,
		TaskResultFetcher: &TaskResultFetcher{
			ErrChan: make(chan error, 1),
			Ctx:     ctx,
		},
		Ctx:       ctx,
		CtxCancel: cancel,
	}
}

func (maintainer *ScheduleMaintainer) SubmitTask(ctx context.Context, conn *net.TCPConn, br *bufio.Reader) (*Task, error) {
	task := makeTask(conn, br)

	select {
	case maintainer.PendingTasks <- task:
		return task, nil
	case <-ctx.Done():
		return nil, context.Canceled
	default:
		return nil, errors.New("task queue full")
	}
}

// 耗时操作, 建立连接和main stream, 后续启动一个goroutine状态机维护连接状态, 持续处理转发任务
func RunBackgroundThreadForever() (scheduer *ScheduleMaintainer, err error) {
	var control *ControlConnContext
	for i := 0; i < Config.ConnectMaxRetry+1; i++ {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*time.Duration(Config.ConnectTimeoutMs)))
		control, err = MakeControlConnContext(ctx)
		if err != nil {
			DebugLogger.Printf("failed to connect proxy server, retry %dst...", i+1)
			cancel()
			continue
		}
		cancel()
		break
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect proxy server: %w", err)
	}

	maintainer := MakeScheduleMatainer(control, 128)
	go maintainer.RunForeverWithInitialCtx()
	return maintainer, nil
}
