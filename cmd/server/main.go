package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/itsHabib/collector/internal/api"
	"github.com/itsHabib/collector/internal/server"
	"github.com/itsHabib/collector/internal/statsd/collector"
	"github.com/itsHabib/collector/internal/statsd/query"
	"github.com/itsHabib/collector/internal/storage"
)

func main() {
	l, err := zap.NewProduction(zap.AddCaller())
	if err != nil {
		log.Fatalf("Could not create logger: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := storage.NewSkipListStorage()
	c := collector.NewCollector("localhost:8125", 1*time.Second, s, l, true)
	querySvc := query.NewService(l, s)
	queryApi := api.NewQuery(l, querySvc)
	statsAPI := api.NewStats(l, c)
	svr := server.NewServer(l, c, queryApi, statsAPI)

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		<-sigC
		cancel()
		signal.Stop(sigC)
		close(sigC)
		_ = svr.Shutdown(ctx)
	}()

	svr.Run(ctx)
}

func printMemUsage() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
			fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
			fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
			fmt.Printf("\tHeap Allc = %v MiB", bToMb(m.HeapAlloc))
			fmt.Printf("\tHeap Sys = %v MiB", bToMb(m.HeapSys))
			fmt.Printf("\tHeap Inuse = %v MiB", bToMb(m.HeapInuse))
			fmt.Printf("\tHeap Objects = %v", m.HeapObjects)
			fmt.Printf("\tNumGC = %v\n", m.NumGC)
		}
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
