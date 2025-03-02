package main

import (
	"context"
	"flag"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var connStr string
	flag.StringVar(&connStr, "connStr", "postgres://myuser:mypassword@localhost:5432/mydatabase", "PostgreSQL URL")
	flag.Parse()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("failed pool", err)
	}

	con, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to Acquire", err)
	}
	accountDDL := `
    CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    debits_pending BIGINT NOT NULL DEFAULT 0,
    debits_posted BIGINT NOT NULL DEFAULT 0,
    credits_pending BIGINT NOT NULL DEFAULT 0,
    credits_posted BIGINT NOT NULL DEFAULT 0,
    user_data128 UUID,
    user_data64 BIGINT,
    user_data32 INTEGER,
    reserved INTEGER,
    ledger INTEGER,
    code SMALLINT,
    flags SMALLINT,
    timestamp BIGINT
    );
    `

	transferDDL := `
  CREATE TABLE transfers (
    id UUID PRIMARY KEY,
    debit_account_id UUID NOT NULL REFERENCES accounts(id),
    credit_account_id UUID NOT NULL REFERENCES accounts(id),
    amount BIGINT NOT NULL,
    ledger INTEGER,
    code SMALLINT,
    flags SMALLINT,
    timestamp BIGINT
  );
  `
	resp, err := con.Exec(ctx, accountDDL)
	log.Println(resp, err)
	resp, err = con.Exec(ctx, transferDDL)
	log.Println(resp, err)

}
