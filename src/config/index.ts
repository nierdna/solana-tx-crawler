import dotenv from 'dotenv';

dotenv.config();

export const config = {
  solana: {
    rpcUrl: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    programAddress: process.env.PROGRAM_ADDRESS || 'oreV3EG1i9BEgiAJ8b177Z2S2rMarzak4NMv1kULvWv',
  },
  mongodb: {
    uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
    database: process.env.MONGODB_DATABASE || 'solana_crawler',
  },
  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://localhost:5672',
  },
  crawler: {
    batchSize: parseInt(process.env.BATCH_SIZE || '100', 10),
    signaturesBatchSize: parseInt(process.env.SIGNATURES_BATCH_SIZE || '1000', 10),
    requestDelayMs: parseInt(process.env.REQUEST_DELAY_MS || '100', 10),
    skipBackfill: process.env.SKIP_BACKFILL === 'true',
  },
  retry: {
    maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10),
    retryDelayMs: parseInt(process.env.RETRY_DELAY_MS || '1000', 10),
  },
};

