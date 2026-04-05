package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func main() {
	var totalAccount, totalTransfer, lastTransferId, concurrency, batchSize int
	var tbAddress string
	flag.IntVar(&totalAccount, "totalAccount", 10_000_000, "total accounts")
	flag.IntVar(&totalTransfer, "totalTransfer", 1_000_000, "total transfers")
	flag.IntVar(&lastTransferId, "lastTransferId", 0, "last transfer id")
	flag.IntVar(&concurrency, "concurrency", 1, "worker goroutines")
	flag.IntVar(&batchSize, "batchSize", 8190, "batch size")
	flag.StringVar(&tbAddress, "tbAddress", "3000", "tigerbeetle address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startTime := time.Now()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil { log.Fatalf("client: %v", err) }
	defer client.Close()

	top20 := int(float64(totalAccount) * 0.2)
	if top20 < 1 { top20 = 1 }
	bottom80 := totalAccount - top20

	transferCh := make(chan Transfer, 1000)
	batchCh := make(chan []Transfer, concurrency)

	var totalErr, totalOK int64

	go func() {
		defer close(transferCh)
		count80 := int(float64(totalTransfer) * 0.8)
		for i := 0; i < totalTransfer; i++ {
			select {
			case <-ctx.Done(): return
			default:
			}
			var deb, cred uint64
			if i < count80 {
				deb = uint64(rand.Intn(top20) + 1)
			} else {
				deb = uint64(rand.Intn(bottom80) + top20 + 1)
			}
			cred = uint64(rand.Intn(totalAccount) + 1)
			transferCh <- Transfer{
				ID:              ToUint128(uint64(i + lastTransferId + 1)),
				DebitAccountID:  ToUint128(deb),
				CreditAccountID: ToUint128(cred),
				Amount:          ToUint128(1000),
				Ledger:          20,
				Code:            1,
			}
		}
	}()

	go func() {
		defer close(batchCh)
		batch := make([]Transfer, 0, batchSize)
		for t := range transferCh {
			batch = append(batch, t)
			if len(batch) == batchSize {
				b := make([]Transfer, batchSize)
				copy(b, batch)
				batchCh <- b
				batch = batch[:0]
			}
		}
		if len(batch) > 0 { batchCh <- batch }
	}()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				errs, err := client.CreateTransfers(batch)
				if err != nil { log.Printf("error: %v", err) }
				atomic.AddInt64(&totalErr, int64(len(errs)))
				atomic.AddInt64(&totalOK, int64(len(batch)-len(errs)))
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(startTime)
	rps := float64(totalOK) / elapsed.Seconds()
	log.Printf("done: ok=%d err=%d time=%s RPS=%.0f",
		totalOK, totalErr, elapsed.Round(time.Millisecond), rps)
}
