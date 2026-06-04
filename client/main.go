package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %v client.conf", os.Args[0])
	}

	if err := LoadConfig(os.Args[1]); err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	scheduler, err := RunBackgroundThreadForever()
	if err != nil {
		log.Fatalf("failed to run background thread: %v", err)
	}

	// 启动本地http服务器，这是阻塞操作
	RunHttpProxyServer(scheduler)
}
