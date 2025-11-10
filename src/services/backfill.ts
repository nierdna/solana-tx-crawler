import { SolanaClient } from './solana-client';
import { MongoManager } from '../database/mongo';
import { logger } from '../utils/logger';
import { Transaction, CrawlState } from '../database/schemas';
import { ConfirmedSignatureInfo } from '@solana/web3.js';

export class BackfillService {
  private solanaClient: SolanaClient;
  private mongoManager: MongoManager;
  private shouldStop: boolean = false;

  constructor(solanaClient: SolanaClient, mongoManager: MongoManager) {
    this.solanaClient = solanaClient;
    this.mongoManager = mongoManager;
  }

  /**
   * Start backfill process
   * Crawls from latest to oldest transactions using the 'before' flag
   */
  async start(): Promise<void> {
    logger.info('Starting backfill service...');

    try {
      // Check if there's an existing backfill in progress
      const existingState = await this.mongoManager.getCrawlState('backfill');
      
      let beforeSignature: string | undefined;
      let totalProcessed = 0;
      let backfillStartSignature: string | undefined;

      if (existingState && existingState.status === 'in_progress') {
        logger.info(`Resuming backfill from signature: ${existingState.lastSignature}`);
        beforeSignature = existingState.lastSignature;
        totalProcessed = existingState.totalProcessed || 0;
        backfillStartSignature = existingState.backfillStartSignature;
      } else {
        logger.info('Starting new backfill from latest transactions');
        
        // SAVE THE VERY FIRST SIGNATURE (absolute latest at start time)
        // This will be used by forward crawler to fill the gap
        const firstBatch = await this.solanaClient.getSignaturesForAddress({ limit: 1 });
        if (firstBatch.length > 0) {
          backfillStartSignature = firstBatch[0].signature;
          logger.info(`Saved backfill start signature: ${backfillStartSignature}`);
          logger.info(`This signature will be used to fill gap after backfill completes`);
        }
      }

      let hasMore = true;
      let iterationCount = 0;

      while (hasMore && !this.shouldStop) {
        iterationCount++;
        logger.info(`Backfill iteration ${iterationCount}, processed so far: ${totalProcessed}`);

        // Fetch signatures
        const signatures = await this.solanaClient.getSignaturesForAddress({
          before: beforeSignature,
        });

        if (signatures.length === 0) {
          logger.info('No more signatures to fetch. Backfill complete!');
          hasMore = false;
          break;
        }

        logger.info(`Fetched ${signatures.length} signatures in this batch`);

        // Process this batch of signatures
        const processedCount = await this.processSignaturesBatch(signatures);
        totalProcessed += processedCount;

        // Update state with the last signature in this batch
        const lastSignature = signatures[signatures.length - 1];
        await this.updateBackfillState(
          lastSignature.signature,
          lastSignature.slot || 0,
          'in_progress',
          totalProcessed,
          backfillStartSignature
        );

        // Set beforeSignature for next iteration
        beforeSignature = lastSignature.signature;

        logger.info(
          `Processed ${processedCount} transactions. ` +
          `Total: ${totalProcessed}. Last signature: ${lastSignature.signature}`
        );
      }

      if (this.shouldStop) {
        logger.info('Backfill stopped by user. State has been saved.');
      } else {
        // Mark backfill as completed
        if (beforeSignature) {
          await this.updateBackfillState(
            beforeSignature,
            0,
            'completed',
            totalProcessed,
            backfillStartSignature
          );
        }
        logger.info(`Backfill completed! Total transactions processed: ${totalProcessed}`);
        if (backfillStartSignature) {
          logger.info(`Backfill start signature saved: ${backfillStartSignature}`);
          logger.info(`Forward crawler will use this to fill any gap`);
        }
      }
    } catch (error) {
      logger.error('Backfill service encountered an error', error);
      throw error;
    }
  }

  /**
   * Process a batch of signatures
   */
  private async processSignaturesBatch(
    signatures: ConfirmedSignatureInfo[]
  ): Promise<number> {
    // Extract signature strings
    const signatureStrings = signatures.map(s => s.signature);

    // Fetch parsed transactions
    const parsedTransactions = await this.solanaClient.getParsedTransactions(signatureStrings);

    // Prepare transaction documents
    const transactions: Transaction[] = [];
    
    for (let i = 0; i < signatures.length; i++) {
      const sigInfo = signatures[i];
      const parsedTx = parsedTransactions[i];

      if (!parsedTx) {
        logger.warn(`Failed to fetch parsed transaction for signature: ${sigInfo.signature}`);
        continue;
      }

      transactions.push({
        signature: sigInfo.signature,
        slot: sigInfo.slot,
        blockTime: sigInfo.blockTime as any,
        err: sigInfo.err,
        parsedData: parsedTx,
        createdAt: new Date(),
      });
    }

    // Save to MongoDB in batch
    if (transactions.length > 0) {
      await this.mongoManager.saveTransactionsBatch(transactions);
      logger.info(`Saved ${transactions.length} transactions to database`);
    }

    return transactions.length;
  }

  /**
   * Update backfill state
   */
  private async updateBackfillState(
    lastSignature: string,
    lastSlot: number,
    status: 'in_progress' | 'completed',
    totalProcessed: number,
    backfillStartSignature?: string
  ): Promise<void> {
    const state: CrawlState = {
      type: 'backfill',
      lastSignature,
      lastSlot,
      status,
      totalProcessed,
      backfillStartSignature,
      updatedAt: new Date(),
    };

    await this.mongoManager.updateCrawlState(state);
  }

  /**
   * Stop the backfill service gracefully
   */
  stop(): void {
    logger.info('Stopping backfill service...');
    this.shouldStop = true;
  }
}

