package main

import (
	"log"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"
)

func perf() {
	http.ListenAndServe("localhost:6061", nil)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %v client.conf", os.Args[0])
	}

	if err := LoadConfig(os.Args[1]); err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if Config.Debug {
		go func() {
			perf()
		}()

		go func() {
			for {
				time.Sleep(time.Second)
				DebugLogger.Printf("current connetion cnt from browser is: %v\n", ConnCnt.Load())
			}
		}()
	}

	scheduler, err := RunBackgroundThreadForever()
	if err != nil {
		log.Fatalf("failed to run background thread: %v", err)
	}

	// 启动本地http服务器，这是阻塞操作
	RunHttpProxyServer(scheduler)
}
