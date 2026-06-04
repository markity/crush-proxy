package main

import (
	"bufio"
	"context"
	"net/http"
)

type TaskType int

const (
	UndefinedTask TaskType = iota
	HttpRequestTask
	ConnectTask
)

type Task struct {
	Request       *http.Request
	ConnBufReader *bufio.Reader

	ctx       context.Context
	ctxCancel context.CancelCauseFunc
	recvChan  chan *FrameData
}

func MakeTask(req *http.Request, connBufReader *bufio.Reader) *Task {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Task{
		Request:       req,
		ConnBufReader: connBufReader,
		ctx:           ctx,
		ctxCancel:     cancel,
	}
}

func (task *Task) Stop(err error) {
	task.ctxCancel(err)
}
