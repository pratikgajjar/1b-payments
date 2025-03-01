package main

import (
	"log"
	"math/rand"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

var BATCH_SIZE int = 8190

func main() {
	tbAddress := "3000"
	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()

	transfers := []Transfer{}
	totalAccount := 10_000_000  // same no of transfers
	totalTransfer := 10_000_000 // same no of transfers
	top20 := int(float64(totalAccount) * 0.2)
	bottom80 := totalAccount - top20
	// 80% of transactions from 20% of accounts
	for i := 0; i < int(float64(totalTransfer)*0.8); i++ {
		transfers = append(transfers, Transfer{
			ID:              ToUint128(uint64(i + 1)),
			DebitAccountID:  ToUint128(uint64((i % top20) + 1)),
			CreditAccountID: ToUint128(uint64(rand.Intn(totalAccount))),
			Amount:          ToUint128(1000),
			Ledger:          20,
			Code:            1,
		})
	}

	// Remaining 20% of transactions from 80% of accounts
	for i := int(float64(totalAccount) * 0.8); i < totalAccount; i++ {
		transfers = append(transfers, Transfer{
			ID:              ToUint128(uint64(i + 1)),
			DebitAccountID:  ToUint128(uint64((i % bottom80) + top20)),
			CreditAccountID: ToUint128(uint64(rand.Intn(totalAccount))),
			Amount:          ToUint128(500),
			Ledger:          20,
			Code:            1,
		})
	}
	totalError := 0
	for i := 0; i < len(transfers); i += BATCH_SIZE {
		size := BATCH_SIZE
		if i+BATCH_SIZE > len(transfers) {
			size = len(transfers) - i
		}
		transferErrors, err := client.CreateTransfers(transfers[i : i+size])
		if err != nil {
			log.Println(err)
		}
		if len(transferErrors) > 0 {
			log.Println(transferErrors)
			totalError += len(transferErrors)
		}
		_, _ = transferErrors, err // Error handling omitted.
	}
	log.Println("Transfer Complete count=", len(transfers), "err=", totalError)
}
