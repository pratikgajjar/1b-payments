package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Account struct {
	ID             uuid.UUID
	DebitsPending  uint64
	DebitsPosted   uint64
	CreditsPending uint64
	CreditsPosted  uint64
	UserData128    uuid.UUID
	UserData64     uint64
	UserData32     uint32
	Reserved       uint32
	Ledger         uint32
	Code           uint16
	Flags          uint16
	Timestamp      uint64
}

func main() {
	var totalAccounts, batchSize, concurrency int
	var connStr string

	flag.IntVar(&totalAccounts, "totalAccounts", 10_000_000, "Total accounts")
	flag.IntVar(&batchSize, "batchSize", 8190, "Batch size")
	flag.IntVar(&concurrency, "concurrency", 4, "Concurrency")
	flag.StringVar(&connStr, "connStr", "postgres://myuser:mypassword@localhost:5432/mydatabase", "PostgreSQL URL")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down gracefully...")
		cancel()
	}()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	start := time.Now()
	var wg sync.WaitGroup
	accountCh := make(chan []Account, concurrency)

	// Account generator
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(accountCh)

		for i := 0; i < totalAccounts; i += batchSize {
			select {
			case <-ctx.Done():
				return
			default:
				currentBatchSize := min(batchSize, totalAccounts-i)
				batch := make([]Account, currentBatchSize)

				for j := 0; j < currentBatchSize; j++ {
					id, _ := uuid.NewV7()
					batch[j] = Account{
						ID:        id,
						Ledger:    20,
						Code:      2,
						Timestamp: uint64(time.Now().UnixNano()),
					}
				}
				accountCh <- batch
			}
		}
	}()

	var inserted int64
	// Workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				log.Printf("Worker %d: pool acquire failed: %v", workerID, err)
				return
			}
			defer conn.Release()

			// Prepare the SQL statement
			const insertSQL = `
				INSERT INTO accounts (
					id, debits_pending, debits_posted,
					credits_pending, credits_posted, user_data128,
					user_data64, user_data32, reserved, ledger,
					code, flags, timestamp
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
				)
			`

			for batch := range accountCh {
				for _, acc := range batch {
					// Execute individual INSERT statements in auto-commit mode
					_, err := conn.Exec(ctx, insertSQL,
						acc.ID,
						acc.DebitsPending,
						acc.DebitsPosted,
						acc.CreditsPending,
						acc.CreditsPosted,
						acc.UserData128,
						acc.UserData64,
						acc.UserData32,
						acc.Reserved,
						acc.Ledger,
						acc.Code,
						acc.Flags,
						acc.Timestamp,
					)

					if err != nil {
						log.Printf("Worker %d: insert failed: %v", workerID, err)
					} else {
						atomic.AddInt64(&inserted, 1)
					}
				}
			}
		}(i)
	}

	// Start monitoring goroutine to show progress
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := atomic.LoadInt64(&inserted)
				if current > 0 {
					elapsed := time.Since(start)
					rate := float64(current) / elapsed.Seconds()
					log.Printf("Progress: %d/%d accounts (%.2f inserts/sec)",
						current, totalAccounts, rate)
				}
			}
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	rate := float64(inserted) / elapsed.Seconds()

	log.Printf("Completed: Inserted %d accounts in %s (%.2f inserts/sec)",
		inserted, elapsed, rate)
	log.Printf("Average time per insert: %.6f ms",
		(elapsed.Seconds()*1000)/float64(inserted))
}
