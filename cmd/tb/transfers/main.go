package main

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

var (
	BATCH_SIZE  int = 8190
	concurrency int = 10 // number of worker goroutines processing batches concurrently
)

func main() {
	tbAddress := "3000"
	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()

	totalAccount := 10_000_000    // total number of accounts
	totalTransfer := totalAccount // total number of transfers
	lastTransferId := 10_000_020
	top20 := int(float64(totalAccount) * 0.2)
	bottom80 := totalAccount - top20

	// Channels to stream transfers and batches.
	transferCh := make(chan Transfer, 1000)       // buffered channel for transfers
	batchCh := make(chan []Transfer, concurrency) // buffered channel for batches

	var totalError int64
	var totalSuccess int64

	// Producer: generate transfers without storing all in memory.
	go func() {
		// 80% of transactions from the top 20% of accounts.
		count80 := int(float64(totalTransfer) * 0.8)
		for i := 0; i < count80; i++ {
			t := Transfer{
				ID:              ToUint128(uint64(i + lastTransferId + 1)),
				DebitAccountID:  ToUint128(uint64((i % top20) + 1)),
				CreditAccountID: ToUint128(uint64(rand.Intn(totalAccount))),
				Amount:          ToUint128(1000),
				Ledger:          20,
				Code:            1,
			}
			transferCh <- t
		}
		// Remaining 20% of transactions from the bottom 80% of accounts.
		for i := count80; i < totalTransfer; i++ {
			t := Transfer{
				ID:              ToUint128(uint64(i + lastTransferId + 1)),
				DebitAccountID:  ToUint128(uint64((i % bottom80) + top20)),
				CreditAccountID: ToUint128(uint64(rand.Intn(totalAccount))),
				Amount:          ToUint128(500),
				Ledger:          20,
				Code:            1,
			}
			transferCh <- t
		}
		// No more transfers to produce.
		close(transferCh)
	}()

	// Batch collector: groups transfers into batches.
	go func() {
		batch := make([]Transfer, 0, BATCH_SIZE)
		for t := range transferCh {
			batch = append(batch, t)
			if len(batch) == BATCH_SIZE {
				// Send a copy of the current batch.
				b := make([]Transfer, BATCH_SIZE)
				copy(b, batch)
				batchCh <- b
				batch = batch[:0]
			}
		}
		// Send any remaining transfers as a final batch.
		if len(batch) > 0 {
			b := make([]Transfer, len(batch))
			copy(b, batch)
			batchCh <- b
		}
		close(batchCh)
	}()

	// Worker pool: process each batch concurrently.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range batchCh {
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
		}(i)
	}

	// Wait for all worker goroutines to finish processing.
	wg.Wait()

	log.Println("Transfer Complete",
		"total transfers processed=", totalTransfer,
		"success=", atomic.LoadInt64(&totalSuccess),
		"errors=", atomic.LoadInt64(&totalError))
}
