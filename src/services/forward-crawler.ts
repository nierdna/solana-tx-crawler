import { SolanaClient } from './solana-client';
import { MongoManager } from '../database/mongo';
import { logger } from '../utils/logger';
import { Transaction, CrawlState } from '../database/schemas';
import { ConfirmedSignatureInfo } from '@solana/web3.js';

export class ForwardCrawler {
  private solanaClient: SolanaClient;
  private mongoManager: MongoManager;
  private shouldStop: boolean = false;

  constructor(solanaClient: SolanaClient, mongoManager: MongoManager) {
    this.solanaClient = solanaClient;
    this.mongoManager = mongoManager;
  }

  /**
   * Start forward crawling
   * Crawls from the latest signature in DB to current
   * Uses backfillStartSignature from backfill state to fill any gap
   */
  async start(): Promise<void> {
    logger.info('Starting forward crawler...');

    try {
      // Get backfill state to check for backfillStartSignature
      const backfillState = await this.mongoManager.getCrawlState('backfill');
      
      if (!backfillState) {
        logger.warn('No backfill state found. Run backfill first.');
        return;
      }

      // Determine the target signature (where backfill started)
      let targetSignature: string;
      
      if (backfillState.backfillStartSignature) {
        // Use the saved start signature (most accurate)
        targetSignature = backfillState.backfillStartSignature;
        logger.info(`Using backfill start signature as target: ${targetSignature}`);
        logger.info(`This ensures no gap between backfill start and current time`);
      } else {
        // Fallback: use latest from DB (old behavior)
        const latestTx = await this.mongoManager.getLatestTransaction();
        if (!latestTx) {
          logger.warn('No transactions in database and no backfill start signature.');
          return;
        }
        targetSignature = latestTx.signature;
        logger.warn(`No backfill start signature found. Using latest from DB: ${targetSignature}`);
        logger.warn(`This may result in a gap if backfill took a long time.`);
      }

      let totalProcessed = 0;
      let hasMore = true;
      let beforeSignature: string | undefined = undefined; // Start from absolute latest
      let iterationCount = 0;

      logger.info('Starting gap-fill loop...');

      // Loop to fill the gap from current to target signature
      while (hasMore && !this.shouldStop) {
        iterationCount++;
        logger.info(`Forward crawl iteration ${iterationCount}`);

        // Fetch signatures with 'before' for pagination, 'until' as stop point
        const signatures = await this.solanaClient.getSignaturesForAddress({
          before: beforeSignature,
          until: targetSignature,
        });

        if (signatures.length === 0) {
          logger.info('No more new transactions. Gap fully filled!');
          hasMore = false;
          break;
        }

        logger.info(`Found ${signatures.length} new transactions in this batch`);

        // Process the new signatures
        const processedCount = await this.processSignaturesBatch(signatures);
        totalProcessed += processedCount;

        // Update beforeSignature for next iteration
        // Last signature in array is the oldest in this batch
        beforeSignature = signatures[signatures.length - 1].signature;

        logger.info(
          `Processed ${processedCount} transactions. ` +
          `Total new: ${totalProcessed}. Last signature: ${beforeSignature}`
        );
      }

      // Update forward crawler state with the absolute latest
      if (totalProcessed > 0) {
        const absoluteLatest = await this.mongoManager.getLatestTransaction();
        if (absoluteLatest) {
          await this.updateForwardCrawlState(
            absoluteLatest.signature,
            absoluteLatest.slot || 0,
            'completed',
            totalProcessed
          );
        }
      } else {
        logger.info('Database was already up to date. No new transactions.');
      }

      logger.info(`Forward crawl completed! Total processed: ${totalProcessed} new transactions`);
    } catch (error) {
      logger.error('Forward crawler encountered an error', error);
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
      logger.info(`Saved ${transactions.length} new transactions to database`);
    }

    return transactions.length;
  }

  /**
   * Update forward crawl state
   */
  private async updateForwardCrawlState(
    lastSignature: string,
    lastSlot: number,
    status: 'in_progress' | 'completed',
    totalProcessed: number
  ): Promise<void> {
    const state: CrawlState = {
      type: 'forward',
      lastSignature,
      lastSlot,
      status,
      totalProcessed,
      updatedAt: new Date(),
    };

    await this.mongoManager.updateCrawlState(state);
  }

  /**
   * Stop the forward crawler gracefully
   */
  stop(): void {
    logger.info('Stopping forward crawler...');
    this.shouldStop = true;
  }
}

