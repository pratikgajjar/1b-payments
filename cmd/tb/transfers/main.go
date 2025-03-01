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

var (
	BATCH_SIZE  int = 8190
	concurrency int = 2 // number of worker goroutines processing batches concurrently
)

func main() {
	// Parse command-line flags.
	var totalAccount int
	var totalTransfer int
	var lastTransferId int
	flag.IntVar(&totalAccount, "totalAccount", 10000000, "total number of accounts")
	flag.IntVar(&totalTransfer, "totalTransfer", 10000000, "total number of transfers")
	flag.IntVar(&lastTransferId, "lastTransferId", 0, "last transfer ID")
	flag.Parse()

	// Set up context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Record the start time.
	startTime := time.Now()

	// Listen for OS interrupt signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal: %s. Initiating graceful shutdown...", sig)
		cancel()
	}()

	// Initialize TigerBeetle client.
	tbAddress := "3000"
	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()

	// Calculate boundaries.
	top20 := int(float64(totalAccount) * 0.2)
	if top20 < 1 {
		top20 = 1
	}
	bottom80 := totalAccount - top20

	// Channels for streaming transfers and batches.
	transferCh := make(chan Transfer, 1000)       // buffered channel for transfers
	batchCh := make(chan []Transfer, concurrency) // buffered channel for batches

	var totalError int64
	var totalSuccess int64

	// Producer: generate transfers without storing all in memory.
	go func() {
		defer close(transferCh)
		// 80% of transactions: debit from top 20 accounts randomly.
		count80 := int(float64(totalTransfer) * 0.8)
		for i := 0; i < count80; i++ {
			// Check for shutdown signal.
			select {
			case <-ctx.Done():
				log.Println("Producer received shutdown signal")
				return
			default:
			}
			t := Transfer{
				ID:              ToUint128(uint64(i + lastTransferId + 1)),
				DebitAccountID:  ToUint128(uint64(rand.Intn(top20) + 1)),        // random account from 1 to top20
				CreditAccountID: ToUint128(uint64(rand.Intn(totalAccount) + 1)), // random account from 1 to totalAccount
				Amount:          ToUint128(1000),
				Ledger:          20,
				Code:            1,
			}
			transferCh <- t
		}
		// 20% of transactions: debit from bottom 80 accounts randomly.
		for i := count80; i < totalTransfer; i++ {
			select {
			case <-ctx.Done():
				log.Println("Producer received shutdown signal")
				return
			default:
			}
			t := Transfer{
				ID:              ToUint128(uint64(i + lastTransferId + 1)),
				DebitAccountID:  ToUint128(uint64(rand.Intn(bottom80) + top20 + 1)), // random account from top20+1 to totalAccount
				CreditAccountID: ToUint128(uint64(rand.Intn(totalAccount) + 1)),     // random account from 1 to totalAccount
				Amount:          ToUint128(500),
				Ledger:          20,
				Code:            1,
			}
			transferCh <- t
		}
	}()

	// Batch collector: group transfers into batches.
	go func() {
		defer close(batchCh)
		batch := make([]Transfer, 0, BATCH_SIZE)
		for {
			select {
			case <-ctx.Done():
				log.Println("Batch collector received shutdown signal")
				return
			case t, ok := <-transferCh:
				if !ok {
					// Send any remaining transfers.
					if len(batch) > 0 {
						b := make([]Transfer, len(batch))
						copy(b, batch)
						batchCh <- b
					}
					return
				}
				batch = append(batch, t)
				if len(batch) == BATCH_SIZE {
					b := make([]Transfer, BATCH_SIZE)
					copy(b, batch)
					batchCh <- b
					batch = batch[:0]
				}
			}
		}
	}()

	// Worker pool: process each batch concurrently.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Printf("Worker %d received shutdown signal", workerID)
					return
				case batch, ok := <-batchCh:
					if !ok {
						return
					}
					transferErrors, err := client.CreateTransfers(batch)
					if err != nil {
						log.Printf("Worker %d error: %s", workerID, err)
					}
					if len(transferErrors) > 0 {
						log.Printf("Worker %d transfer errors: %v", workerID, transferErrors)
						atomic.AddInt64(&totalError, int64(len(transferErrors)))
						atomic.AddInt64(&totalSuccess, int64(len(batch)-len(transferErrors)))
					} else {
						atomic.AddInt64(&totalSuccess, int64(len(batch)))
					}
				}
			}
		}(i)
	}

	// Wait for all workers to finish.
	wg.Wait()

	// Calculate and log elapsed time.
	elapsed := time.Since(startTime)

	log.Println("Transfer Complete",
		"total transfers processed=", totalTransfer,
		"success=", atomic.LoadInt64(&totalSuccess),
		"errors=", atomic.LoadInt64(&totalError),
		"time=", elapsed,
	)
}
