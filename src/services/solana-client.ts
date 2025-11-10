import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
  ParsedTransactionWithMeta,
  SignaturesForAddressOptions,
} from '@solana/web3.js';
import { config } from '../config';
import { logger } from '../utils/logger';

export class SolanaClient {
  private connection: Connection;
  private programAddress: PublicKey;

  constructor() {
    this.connection = new Connection(config.solana.rpcUrl, 'confirmed');
    this.programAddress = new PublicKey(config.solana.programAddress);
    logger.info(`Initialized Solana client for program: ${config.solana.programAddress}`);
  }

  /**
   * Get signatures for address with retry logic
   */
  async getSignaturesForAddress(
    options?: SignaturesForAddressOptions
  ): Promise<ConfirmedSignatureInfo[]> {
    const limit = Math.min(options?.limit || config.crawler.signaturesBatchSize, 1000);
    
    return this.retryWithBackoff(async () => {
      const signatures = await this.connection.getSignaturesForAddress(
        this.programAddress,
        { ...options, limit }
      );
      
      logger.debug(`Fetched ${signatures.length} signatures`);
      return signatures;
    }, 'getSignaturesForAddress');
  }

  /**
   * Get parsed transactions in batches
   */
  async getParsedTransactions(
    signatures: string[]
  ): Promise<(ParsedTransactionWithMeta | null)[]> {
    const results: (ParsedTransactionWithMeta | null)[] = [];
    const batchSize = config.crawler.batchSize;

    for (let i = 0; i < signatures.length; i += batchSize) {
      const batch = signatures.slice(i, i + batchSize);
      
      const batchResults = await this.retryWithBackoff(async () => {
        const txs = await this.connection.getParsedTransactions(batch, {
          maxSupportedTransactionVersion: 0,
        });
        
        logger.debug(`Fetched ${txs.length} parsed transactions (batch ${i / batchSize + 1})`);
        return txs;
      }, 'getParsedTransactions');

      results.push(...batchResults);

      // Add delay between batches to avoid rate limiting
      if (i + batchSize < signatures.length) {
        await this.sleep(config.crawler.requestDelayMs);
      }
    }

    return results;
  }

  /**
   * Get a single parsed transaction
   */
  async getParsedTransaction(
    signature: string
  ): Promise<ParsedTransactionWithMeta | null> {
    return this.retryWithBackoff(async () => {
      return await this.connection.getParsedTransaction(signature, {
        maxSupportedTransactionVersion: 0,
      });
    }, 'getParsedTransaction');
  }

  /**
   * Retry logic with exponential backoff
   */
  private async retryWithBackoff<T>(
    fn: () => Promise<T>,
    operationName: string
  ): Promise<T> {
    let lastError: Error | undefined;
    
    for (let attempt = 0; attempt <= config.retry.maxRetries; attempt++) {
      try {
        return await fn();
      } catch (error) {
        lastError = error as Error;
        
        if (attempt < config.retry.maxRetries) {
          const delay = config.retry.retryDelayMs * Math.pow(2, attempt);
          logger.warn(
            `${operationName} failed (attempt ${attempt + 1}/${config.retry.maxRetries + 1}), ` +
            `retrying in ${delay}ms: ${lastError.message}`
          );
          await this.sleep(delay);
        }
      }
    }

    logger.error(`${operationName} failed after ${config.retry.maxRetries + 1} attempts`);
    throw lastError;
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get connection for direct access if needed
   */
  getConnection(): Connection {
    return this.connection;
  }
}

