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
	pgx "github.com/jackc/pgx/v5"
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
	flag.StringVar(&connStr, "connStr", "postgres://user:pass@localhost:5432/db", "PostgreSQL URL")
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
				batchSize := min(batchSize, totalAccounts-i)
				batch := make([]Account, batchSize)

				for j := 0; j < batchSize; j++ {
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

			for batch := range accountCh {
				rows := make([][]interface{}, len(batch))
				for i, acc := range batch {
					rows[i] = []interface{}{
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
					}
				}

				_, err := conn.Conn().CopyFrom(
					ctx,
					pgx.Identifier{"accounts"},
					[]string{
						"id", "debits_pending", "debits_posted",
						"credits_pending", "credits_posted", "user_data128",
						"user_data64", "user_data32", "reserved", "ledger",
						"code", "flags", "timestamp",
					},
					pgx.CopyFromRows(rows),
				)

				if err != nil {
					log.Printf("Worker %d: copy failed: %v", workerID, err)
				} else {
					atomic.AddInt64(&inserted, int64(len(batch)))
				}
			}
		}(i)
	}

	wg.Wait()
	log.Printf("Inserted %d accounts in %s", inserted, time.Since(start))
}
