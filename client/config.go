package main

import (
	"crush-proxy/comm"
	"encoding/json"
	"os"
)

var Config struct {
	LocalHost string `json:"local_host"`
	LocalPort int    `json:"local_port"`

	ConnectTimeoutMs int `json:"connect_timeout_ms"`
	ConnectMaxRetry  int `json:"connect_max_retry"`

	ReconnectTimeoutMs int `json:"reconnect_timeout_ms"`
	ReconnectMaxRetry  int `json:"reconnect_max_retry"`

	RemoteHost string `json:"remote_host"`
	RemotePort int    `json:"remote_port"`

	Debug bool `json:"debug"`
}

var DebugLogger *comm.DebugeLogger

func LoadConfig(path string) error {
	config, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(config, &Config); err != nil {
		return err
	}

	DebugLogger = comm.NewDebugeLogger(Config.Debug)

	return nil
}
