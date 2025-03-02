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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Transfer struct {
	ID              uuid.UUID
	DebitAccountID  uuid.UUID
	CreditAccountID uuid.UUID
	Amount          uint64
	Ledger          uint32
	Code            uint16
	Flags           uint16
	Timestamp       uint64
}

func main() {
	var totalTransfers, concurrency int
	var connStr string

	flag.IntVar(&totalTransfers, "totalTransfers", 10_000_000, "Total transfers")
	flag.IntVar(&concurrency, "concurrency", 10, "Concurrency")
	flag.StringVar(&connStr, "connStr", "postgres://myuser:mypassword@127.0.0.1:5432/mydatabase", "PostgreSQL URL")
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

	// Parse the connection string to obtain a configuration object.
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("Unable to parse connection string: %v", err)
	}

	// Define the maximum and minimum connections for the pool.
	config.MaxConns = int32(concurrency) // Maximum number of connections
	config.MinConns = int32(concurrency) // Minimum number of connections

	// Create the connection pool with the modified configuration.
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatalf("Pool creation failed: %v", err)
	}
	defer pool.Close()

	// Fetch all account IDs from database
	startFetch := time.Now()
	log.Println("Fetching account IDs from database...")
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "SELECT id FROM accounts")
	if err != nil {
		log.Fatalf("Failed to query accounts: %v", err)
	}
	defer rows.Close()

	var accountIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			log.Fatalf("Error scanning account ID: %v", err)
		}
		accountIDs = append(accountIDs, id)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error reading account IDs: %v", err)
	}

	totalAccounts := len(accountIDs)
	if totalAccounts == 0 {
		log.Fatal("No accounts found in database")
	}
	log.Println("Total accounts=", totalAccounts)

	// Split accounts into top 20% and bottom 80%
	top20Count := int(float64(totalAccounts) * 0.2)
	if top20Count < 1 {
		top20Count = 1
	}
	top20 := accountIDs[:top20Count]
	bottom80 := accountIDs[top20Count:]

	log.Printf("Fetched %d accounts (top20: %d, bottom80: %d) in %s",
		totalAccounts, len(top20), len(bottom80), time.Since(startFetch))

	start := time.Now()

	var wg sync.WaitGroup
	transferCh := make(chan Transfer, concurrency*2)

	// Transfer generator
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(transferCh)

		generate := func(count int, amount uint64, debitPool []uuid.UUID) {
			for i := 0; i < count; i++ {
				select {
				case <-ctx.Done():
					return
				default:
					t := Transfer{
						ID:              uuid.New(), // Generate new UUIDv7 for transfer ID
						Amount:          amount,
						Ledger:          20,
						Code:            1,
						Timestamp:       uint64(time.Now().UnixNano()),
						DebitAccountID:  debitPool[rand.Intn(len(debitPool))],
						CreditAccountID: accountIDs[rand.Intn(totalAccounts)],
					}
					transferCh <- t
				}
			}
		}

		count80 := int(float64(totalTransfers) * 0.8)
		generate(count80, 1000, top20)
		generate(totalTransfers-count80, 500, bottom80)
	}()

	var processed, errors int64
	// Worker pool
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				log.Printf("Worker %d: acquire failed: %v", workerID, err)
				return
			}
			defer conn.Release()

			for t := range transferCh {
				err := func() error {
					tx, err := conn.Conn().Begin(ctx)
					if err != nil {
						return err
					}
					defer tx.Rollback(ctx)

					// Update debit account
					_, err = tx.Exec(ctx,
						`UPDATE accounts 
						SET debits_posted = debits_posted + $1 
						WHERE id = $2`,
						t.Amount, t.DebitAccountID)
					if err != nil {
						return err
					}

					// Update credit account
					_, err = tx.Exec(ctx,
						`UPDATE accounts 
						SET credits_posted = credits_posted + $1 
						WHERE id = $2`,
						t.Amount, t.CreditAccountID)
					if err != nil {
						return err
					}

					// Insert transfer
					_, err = tx.Exec(ctx,
						`INSERT INTO transfers 
						(id, debit_account_id, credit_account_id, amount, 
						ledger, code, flags, timestamp)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
						t.ID, t.DebitAccountID, t.CreditAccountID, t.Amount,
						t.Ledger, t.Code, t.Flags, t.Timestamp)
					if err != nil {
						return err
					}

					return tx.Commit(ctx)
				}()

				if err != nil {
					atomic.AddInt64(&errors, 1)
				} else {
					atomic.AddInt64(&processed, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	log.Printf("Processed %d transfers, errors: %d, took: %s",
		processed, errors, time.Since(start))
}
