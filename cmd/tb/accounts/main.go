package main

import (
	"log"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func main() {
	tbAddress := "3000"
	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()

	accounts := []Account{}

	startAt := 1
	endAt := 10_000_000

	for i := startAt; i < endAt; i++ {
		acc := Account{
			ID:     ToUint128(uint64(i + 1)),
			Ledger: 20,
			Code:   2,
		}
		accounts = append(accounts, acc)
	}

	BATCH_SIZE := 8190
	for i := 0; i < len(accounts); i += BATCH_SIZE {
		size := BATCH_SIZE
		if i+BATCH_SIZE > len(accounts) {
			size = len(accounts) - i
		}
		log.Println(accounts[i])
		accountErrors, err := client.CreateAccounts(accounts[i : i+size])
		if err != nil {
			log.Println(err)
		}
		_, _ = accountErrors, err // Error handling omitted.
	}

	log.Println("Created 1M")
}
