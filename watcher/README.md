# Solana MEV watcher

`Watcher` provides several commands to monitor and analyze Sandwich activities on the Solana blockchain.

## Setup environment and configs

Refer to [.env.example](./.env.example) and create your own `.env` file.
Refer to [config.example.yaml](./config.example.yaml) and create your own `config.yaml` file.

## Build database

## Setup Golang environment and build the project

We use a `go 1.24` version. Install it from [golang offical website](https://go.dev/dl/).
Make sure your `GOPATH` and `GOROOT` are set correctly.

Build the project:

```bash
go mod tidy
go build .
```

## Commands

You can refer to the `./scripts` folder for example scripts to run the commands below.

### Fetch Jito bundles

`jito` command fetches Jito bundles from the Jito API and stores them into the database.
Currently, it fetches use `jito/recent` API to fetch "latest" bundles. But we find that it may miss some bundles. To avoid missing bundles, we should switch to `jito/slot` API to fetch bundles slot by slot.

```bash
go run main.go jito
```

### Fetch Solana slot leader

`slot-info` command fetches Solana slot leaders from the Solana RPC and stores them into the database. It use the official Solana RPC `getSlotLeader` API to fetch each slot's leader.

You can specify the start slot. If not specified, it will fetch from the latest slot to the current slot.

```bash
go run main.go slot-info -s <start_slot>
```

### Fetch Solana transactions and find Sandwiches (under development)

`sandwich-info` command fetches Solana transactions (in each block) from the Solana RPC.
Then it analyzes the transactions to find Sandwich activities and stores them into the database.

```bash
go run main.go sandwich-info -s <start_slot>
```

We focus on both InBlock Sandwich and CrossBlock Sandwich.

The fetch process is done slot by slot, in parallel with multiple workers.
The sandwich analysis is done also in parallel with multiple workers.

You can specify the start slot. If not specified, it will fetch from the latest slot to the current slot.
