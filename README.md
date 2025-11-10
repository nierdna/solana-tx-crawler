# Solana Transaction Crawler

A TypeScript-based crawler for Solana transactions that fetches and stores parsed transaction data from a specific program into MongoDB.

## Features

- **Backfill Mode**: Crawls from latest to oldest transaction using pagination with `before` flag
- **Resume Support**: Can resume from last saved state if interrupted
- **Forward Crawl**: After backfill, crawls from latest saved transaction to current
- **Batching**: Efficient batching for both signatures and transactions
- **Retry Logic**: Exponential backoff retry mechanism for RPC errors
- **Graceful Shutdown**: Saves state before exiting on SIGINT/SIGTERM
- **MongoDB Storage**: Stores all parsed transaction data with indexes

## Project Structure

```
solana-tx-crawler/
├── src/
│   ├── config/
│   │   └── index.ts          # Configuration management
│   ├── database/
│   │   ├── mongo.ts           # MongoDB connection and operations
│   │   └── schemas.ts         # Data schemas and interfaces
│   ├── services/
│   │   ├── solana-client.ts   # Solana RPC wrapper
│   │   ├── backfill.ts        # Backfill service
│   │   └── forward-crawler.ts # Forward crawler
│   ├── utils/
│   │   └── logger.ts          # Winston logger
│   └── index.ts               # Main entry point
├── package.json
├── tsconfig.json
└── README.md
```

## Prerequisites

- Node.js >= 18
- MongoDB instance
- Solana RPC endpoint

## Installation

```bash
pnpm install
```

## Configuration

Create a `.env` file in the root directory:

```env
# Solana RPC Configuration
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com

# Program to crawl
PROGRAM_ADDRESS=oreV3EG1i9BEgiAJ8b177Z2S2rMarzak4NMv1kULvWv

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=solana_crawler

# Crawler Configuration
BATCH_SIZE=100
SIGNATURES_BATCH_SIZE=1000
REQUEST_DELAY_MS=100

# Retry Configuration
MAX_RETRIES=3
RETRY_DELAY_MS=1000
```

## Usage

### Build

```bash
pnpm run build
```

### Run

```bash
# Development mode (with ts-node)
pnpm run dev

# Production mode
pnpm start
```

### Clean

```bash
pnpm run clean
```

## How It Works

### 1. Backfill Phase

- Checks if there's an existing backfill in progress (resume support)
- Fetches signatures in batches of 1000 (Solana's max limit)
- For each batch:
  - Gets parsed transactions in sub-batches of 100
  - Saves transactions to MongoDB
  - Updates crawl state with last signature
- Continues until no more signatures are found
- Marks backfill as completed

### 2. Forward Crawl Phase

- Runs after backfill is complete
- Gets the latest signature from database
- Fetches new signatures using `until` flag
- Processes and saves new transactions
- Updates forward crawl state

### Graceful Shutdown

Press `Ctrl+C` to trigger graceful shutdown. The crawler will:
- Stop fetching new data
- Save current state to MongoDB
- Close database connection
- Exit cleanly

You can resume from where it stopped by running the crawler again.

## MongoDB Collections

### `transactions`

Stores parsed transaction data:
- `signature` (unique): Transaction signature
- `slot`: Slot number
- `blockTime`: Unix timestamp
- `err`: Error if transaction failed
- `parsedData`: Full parsed transaction object
- `createdAt`: When the record was created

### `crawl_state`

Tracks crawler progress:
- `type`: "backfill" or "forward"
- `lastSignature`: Last processed signature
- `lastSlot`: Last processed slot
- `status`: "in_progress" or "completed"
- `totalProcessed`: Total transactions processed
- `updatedAt`: Last update timestamp

## Monitoring

The crawler logs detailed information:
- Progress updates
- Statistics (total transactions, date ranges)
- Errors with retry attempts
- Database operations

Log files:
- `combined.log`: All logs
- `error.log`: Error logs only

## Error Handling

- **RPC Errors**: Automatic retry with exponential backoff
- **Rate Limiting**: Configurable delays between requests
- **Database Errors**: Logged and thrown
- **Network Issues**: Retried up to MAX_RETRIES times

## Performance Tips

1. Use a paid RPC endpoint for better rate limits
2. Adjust `BATCH_SIZE` and `REQUEST_DELAY_MS` based on your RPC limits
3. Monitor MongoDB performance and add indexes as needed
4. Consider running on a server with good network connectivity

## License

MIT

