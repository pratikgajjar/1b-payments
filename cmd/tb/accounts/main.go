package main

import (
	"flag"
	"log"
	"time"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func main() {
	// Define command-line flags.
	var tbAddress string
	var startAt, endAt, batchSize int
	flag.StringVar(&tbAddress, "tbAddress", "3000", "TigerBeetle server address")
	flag.IntVar(&startAt, "startAt", 1, "Starting account ID (inclusive)")
	flag.IntVar(&endAt, "endAt", 10000000, "Ending account ID (exclusive)")
	flag.IntVar(&batchSize, "batchSize", 8190, "Batch size for account creation")
	flag.Parse()

	// Create a TigerBeetle client.
	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()

	// Record the start time.
	startTime := time.Now()

	// Create accounts.
	var accounts []Account
	for i := startAt; i <= endAt; i++ {
		acc := Account{
			ID:     ToUint128(uint64(i)),
			Ledger: 20,
			Code:   2,
		}
		accounts = append(accounts, acc)
	}

	success := 0
	failed := 0
	// Process accounts in batches.
	for i := 0; i < len(accounts); i += batchSize {
		size := batchSize
		if i+batchSize > len(accounts) {
			size = len(accounts) - i
		}
		accountErrors, err := client.CreateAccounts(accounts[i : i+size])
		if err != nil {
			log.Printf("Error in batch starting with account %v: %s", accounts[i], err)
		}
		failed += len(accountErrors)
		success += size - len(accountErrors)
		// For brevity, error handling for individual account errors is omitted.
		_, _ = accountErrors, err
	}

	// Calculate and log elapsed time.
	elapsed := time.Since(startTime)
	log.Printf("Created accounts from %d to %d (total: %d, failed: %d). Time taken: %s", startAt, endAt, success, failed, elapsed)
}
