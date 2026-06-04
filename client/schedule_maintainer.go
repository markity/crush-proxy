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

	log.Printf("new conn success")

	// main stream是命令/心跳/鉴权的专用stream，先建立起来，并且互发一次心跳
	result.MainStream, err = result.Conn.NewStream(ctx)
	if err != nil {
		errOut = fmt.Errorf("failed to make main stream: %w", err)
		return
	}
	log.Printf("new stream success")

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
		log.Println("handshake heartbeat recv")
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
		log.Println("handshake heartbeat sent")
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
	log.Println("send and recv all done")

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

	log.Println("heartbeat is valid, main stream create success")

	errOut = nil
	return
}

type ScheduleMaintainer struct {
	CurrentControl *ControlConnContext
	PendingTasks   chan *Task
}

// 主要事件循环，维护quic conn，main stream活性，处理其它地方submit来的请求创建新的stream做代理
func (maintainer *ScheduleMaintainer) RunForeverWithInitialCtx() {
	// main stream消息循环，task请求循环
	for {
		log.Printf("connection main loop is running, local addr is: %v\n",
			maintainer.CurrentControl.UdpConn.LocalAddr().String())
		// writer和reader will send error when exit
		writerExitChan := make(chan error, 1)
		readerExitChan := make(chan error, 1)

		inChan := make(chan *packet.Packet, 512)
		outChan := make(chan []byte, 512)

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
					log.Printf("heartbeat loss, connection lost")
					connClosed = true
					connCloseReason = errors.New("heartbeat loss")
				}
			case _ = <-inChan:
				tickLoss = 0
			case task := <-maintainer.PendingTasks:
				ctx, timeoutCancel := context.WithTimeout(context.Background(), time.Second*3)
				taskStream, err := maintainer.CurrentControl.Conn.NewStream(ctx)
				taskStream.SetReadContext(task.ctx)
				taskStream.SetWriteContext(task.ctx)
				if err != nil {
					connClosed = true
					connCloseReason = fmt.Errorf("new task stream failed: %w", err)
					timeoutCancel()
					break
				}
				timeoutCancel()
				go HandleTask(task, taskStream)
			case err := <-writerExitChan:
				connClosed = true
				connCloseReason = fmt.Errorf("writer exit: %w", err)
			case err := <-readerExitChan:
				connClosed = true
				connCloseReason = fmt.Errorf("reader exit: %w", err)
			}
		}
		log.Printf("exit main connection loop: %v\n", connCloseReason)

		// 清空资源
		cleanResourceCtx, cleanResourceCtxCancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*500))
		cleanResource(cleanResourceCtx, connCloseReason)
		cleanResourceCtxCancel()

		var err error
		for i := 0; i < Config.ReconnectMaxRetry; i++ {
			fmt.Println(Config.ReconnectMaxRetry)
			log.Printf("connection lost, reconnecting %dst\n", i+1)
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Duration(Config.ConnectTimeoutMs)*time.Millisecond))
			maintainer.CurrentControl, err = MakeControlConnContext(ctx)
			if err == nil {
				cancel()
				log.Printf("reconnect successfully\n")
				break
			}
			cancel()
		}
		if err != nil {
			log.Fatalf("server connection lost: %v", err)
		}
	}
}

// stream的read write ctx已经被设置为task.ctx了
func HandleTask(task *Task, stream *quic.Stream) {
	log.Printf("handle task, task url: %v\n", task.Request.URL.String())
	defer func() {
		task.Request.Body.Close()
		stream.Close()
	}()

	req := task.Request

	// 构造消息头
	headerBs, err := comm.BuildRawRequestHeader(req)
	if err != nil {
		task.Stop(fmt.Errorf("build request header failed: %w", err))
		return
	}

	writerExitChan := make(chan error, 1)
	readerExitChan := make(chan error, 1)

	// writer
	go func() {
		var errChan error
		defer func() {
			writerExitChan <- errChan
			if errChan == nil {
				log.Printf("writer finished it's job")
			}
			close(writerExitChan)
		}()

		log.Printf("write to proxy server: %v\n", string(headerBs))
		_, err := comm.MakeFlushReaderWriter(stream).Write(headerBs)
		if err != nil {
			errChan = err
			return
		}
		// 不断读body, 直到body读到eof，假如body不完全或者tcp意外断开，会产生error
		// TODO:是否读不完body, stream就关了, 此时怎么处理
		req.Body.Close()
		if task.Request.Method != http.MethodConnect {
			n, err := io.Copy(comm.MakeFlushReaderWriter(stream), req.Body)
			log.Printf("copy browser request body to stream: %d %v\n", n, err)
			if errors.Is(err, net.ErrClosed) {
				_, err := io.Copy(io.Discard, req.Body)
				if err != nil && !errors.Is(err, io.EOF) {
					errChan = fmt.Errorf("failed to copy local body to remote stream")
					return
				}
				errChan = nil
				return
			}
			errChan = err
		} else {
			io.Copy(io.Discard, req.Body)
			req.Body.Close()
			n, err := io.Copy(comm.MakeFlushReaderWriter(stream), task.ConnBufReader)
			log.Printf("copy body to stream: %d %v\n", n, err)
			errChan = err
		}
	}()

	// reader
	go func() {
		var errChan error
		defer func() {
			readerExitChan <- errChan
			close(readerExitChan)
			close(task.recvChan)
		}()

		conn := bufio.NewReader(stream)
		// 先找头
		log.Println("waiting for stream's http response header")
		resp, err := http.ReadResponse(conn, req)
		if err != nil {
			log.Printf("handleTask, read response header failed: %v\n", err)
			task.recvChan <- &FrameData{
				Response:         nil,
				Data:             nil,
				IsErr:            true,
				IsResponseHeader: true,
			}
			errChan = fmt.Errorf("failed to read response: %w", err)
			return
		}
		log.Printf("handleTask, got response: %v\n", resp.Status)

		// 正常的头能拿到
		task.recvChan <- &FrameData{
			Response:         resp,
			Data:             nil,
			IsErr:            false,
			IsResponseHeader: true,
		}

		// 开始取body
		for {
			frame := FrameData{}
			buf := make([]byte, 1024)
			n, err := resp.Body.Read(buf)
			if err != nil && !errors.Is(err, io.EOF) {
				readerExitChan <- err
				frame.Data = nil
				frame.IsErr = true

				task.recvChan <- &frame
				errChan = fmt.Errorf("failed to read response body: %w", err)
				return
			}
			log.Println("got data")

			frame.Data = buf[:n]
			frame.IsErr = false
			task.recvChan <- &frame
			if errors.Is(err, io.EOF) {
				return
			}
		}
	}()

	errReader := <-readerExitChan
	if errReader != nil {
		log.Printf("reader failed: %v\n", errReader)
		task.Stop(errReader)
	}
	errWriter := <-writerExitChan
	if errWriter != nil {
		log.Printf("writer failed: %v\n", errWriter)
		task.Stop(errWriter)
	}
	task.Stop(nil)
}

type FrameData struct {
	Response         *http.Response
	Data             []byte
	IsErr            bool // 比如远端的http连接断开，local server需要感知到
	IsResponseHeader bool
}

func MakeScheduleMatainer(control *ControlConnContext, c chan *Task) *ScheduleMaintainer {
	return &ScheduleMaintainer{CurrentControl: control, PendingTasks: c}
}

func (maintainer *ScheduleMaintainer) SubmitTask(task *Task) <-chan *FrameData {
	c := make(chan *FrameData)
	task.recvChan = c
	maintainer.PendingTasks <- task
	return c
}

// 耗时操作, 建立连接和main stream, 后续启动一个goroutine状态机维护连接状态, 持续处理转发任务
func RunBackgroundThreadForever() (scheduer *ScheduleMaintainer, err error) {
	var control *ControlConnContext
	for i := 0; i < Config.ConnectMaxRetry+1; i++ {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*time.Duration(Config.ConnectTimeoutMs)))
		control, err = MakeControlConnContext(ctx)
		if err != nil {
			log.Printf("failed to connect proxy server, retry %dst...", i+1)
			cancel()
			continue
		}
		cancel()
		break
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect proxy server: %w", err)
	}

	maintainer := MakeScheduleMatainer(control, make(chan *Task, 128))
	go maintainer.RunForeverWithInitialCtx()
	return maintainer, nil
}
