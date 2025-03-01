# 1b-payments

1 Billion payments

## Create 10M accounts

```sh
go run ./cmd/tb/accounts/main.go -tbAddress "3000" -startAt 1 endAt 10000000
```

## Create 10M payments

### Sequential

```sh
go run ./cmd/tb/transfers/main.go -totalTransfer 10000000 -lastTransferId 
# change concurrnecy to 1
```

### Concurrent

```sh
sh ./spawn.sh
```

Above spawns 10 go processes each with 2 goroutines to create transfers.
