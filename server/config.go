package main

import (
	"crush-proxy/comm"
	"encoding/json"
	"os"
)

var Config struct {
	ListenHost         string `json:"listen_host"`
	ListenPort         int    `json:"listen_port"`
	HeartbeatTimeoutMs int    `json:"heartbeat_timeout_ms"`
	Cert               string `json:"cert"`
	Key                string `json:"key"`
	Debug              bool   `json:"debug"`
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
