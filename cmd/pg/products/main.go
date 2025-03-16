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

	"github.com/jackc/pgx/v5/pgxpool"
)

type Product struct {
	Name  string
	Price float64
}

func main() {
	var totalProducts, batchSize, concurrency int
	var connStr string

	flag.IntVar(&totalProducts, "totalProducts", 8000, "Total products to insert")
	flag.IntVar(&batchSize, "batchSize", 1000, "Batch size for each worker")
	flag.IntVar(&concurrency, "concurrency", 8, "Number of concurrent workers")
	flag.StringVar(&connStr, "connStr", "postgres://myuser:mypassword@localhost:5432/mydatabase", "PostgreSQL URL")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down gracefully...")
		cancel()
	}()

	// Create connection pool
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	start := time.Now()
	var wg sync.WaitGroup
	productCh := make(chan []Product, concurrency)

	// Product generator
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(productCh)

		for i := 0; i < totalProducts; i += batchSize {
			select {
			case <-ctx.Done():
				return
			default:
				currentBatchSize := min(batchSize, totalProducts-i)
				batch := make([]Product, currentBatchSize)

				for j := 0; j < currentBatchSize; j++ {
					batch[j] = Product{
						Name:  "12345678",
						Price: 123.0,
					}
				}
				productCh <- batch
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

			for batch := range productCh {
				for _, product := range batch {
					_, err := conn.Exec(ctx,
						"INSERT INTO products (name, price) VALUES ($1, $2)",
						product.Name, product.Price)

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
				elapsed := time.Since(start)
				rate := float64(current) / elapsed.Seconds()
				log.Printf("Progress: %d/%d products (%.2f inserts/sec)",
					current, totalProducts, rate)
			}
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	rate := float64(inserted) / elapsed.Seconds()

	log.Printf("Completed: Inserted %d products in %s (%.2f inserts/sec)",
		inserted, elapsed, rate)
	log.Printf("Average time per insert: %.6f ms",
		(elapsed.Seconds()*1000)/float64(inserted))
}
