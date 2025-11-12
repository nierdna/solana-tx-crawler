import { SolanaClient } from './solana-client';
import { MongoManager } from '../database/mongo';
import { logger } from '../utils/logger';
import { config } from '../config';
import { Transaction, CrawlState } from '../database/schemas';
import { ConfirmedSignatureInfo } from '@solana/web3.js';

export class ForwardCrawler {
  private solanaClient: SolanaClient;
  private mongoManager: MongoManager;
  private shouldStop: boolean = false;
  private pollIntervalMs: number = Math.max(config.crawler.requestDelayMs, 1000);

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
      const targetSignature = await this.resolveInitialTarget(backfillState);
      if (!targetSignature) {
        return;
      }

      logger.info('Starting gap-fill loop...');
      const gapProcessed = await this.catchUpToTarget(targetSignature, 'Gap fill');
      await this.recordForwardProgress(gapProcessed);

      if (this.shouldStop) {
        logger.info('Shutdown requested during gap fill. Exiting forward crawler.');
        return;
      }

      logger.info('Entering continuous forward crawl loop...');

      while (!this.shouldStop) {
        const latestTx = await this.mongoManager.getLatestTransaction();

        if (!latestTx) {
          logger.warn('No transactions in database. Waiting before retrying forward crawl.');
          await this.sleep(this.pollIntervalMs);
          continue;
        }

        const processed = await this.catchUpToTarget(latestTx.signature, 'Realtime follow-up');

        if (this.shouldStop) {
          logger.info('Shutdown requested during forward crawl loop.');
          break;
        }

        await this.recordForwardProgress(processed);

        if (processed === 0) {
          logger.debug('No new transactions found. Sleeping before next poll.');
          await this.sleep(this.pollIntervalMs);
        }
      }

      logger.info('Forward crawler loop exited.');
    } catch (error) {
      logger.error('Forward crawler encountered an error', error);
      throw error;
    }
  }

  /**
   * Determine starting target signature for gap fill
   */
  private async resolveInitialTarget(
    backfillState: CrawlState
  ): Promise<string | undefined> {
    if (backfillState.backfillStartSignature) {
      logger.info(`Using backfill start signature as target: ${backfillState.backfillStartSignature}`);
      logger.info('This ensures no gap between backfill start and current time');
      return backfillState.backfillStartSignature;
    }

    const latestTx = await this.mongoManager.getLatestTransaction();
    if (!latestTx) {
      logger.warn('No transactions in database and no backfill start signature.');
      return undefined;
    }

    logger.warn(`No backfill start signature found. Using latest from DB: ${latestTx.signature}`);
    logger.warn('This may result in a gap if backfill took a long time.');
    return latestTx.signature;
  }

  /**
   * Catch up until the provided signature
   */
  private async catchUpToTarget(
    targetSignature: string,
    context: string
  ): Promise<number> {
    let totalProcessed = 0;
    let beforeSignature: string | undefined;
    let iterationCount = 0;

    while (!this.shouldStop) {
      iterationCount++;
      logger.info(`${context} iteration ${iterationCount}`);

      const signatures = await this.solanaClient.getSignaturesForAddress({
        before: beforeSignature,
        until: targetSignature,
      });

      if (signatures.length === 0) {
        if (totalProcessed === 0) {
          logger.info(`${context} found no new transactions. Gap fully filled.`);
        } else {
          logger.info(`${context} gap fully filled.`);
        }
        break;
      }

      logger.info(`${context} fetched ${signatures.length} signatures in this batch`);

      const processedCount = await this.processSignaturesBatch(signatures);
      totalProcessed += processedCount;

      beforeSignature = signatures[signatures.length - 1].signature;

      logger.info(
        `${context} processed ${processedCount} transactions. Total new: ${totalProcessed}. Last signature: ${beforeSignature}`
      );
    }

    return totalProcessed;
  }

  /**
   * Update forward crawl state after processing new transactions
   */
  private async recordForwardProgress(processed: number): Promise<void> {
    if (processed === 0) {
      return;
    }

    const latestTx = await this.mongoManager.getLatestTransaction();
    if (!latestTx) {
      logger.warn('Unable to update forward state: latest transaction not found.');
      return;
    }

    const forwardState = await this.mongoManager.getCrawlState('forward');
    const totalProcessed = (forwardState?.totalProcessed || 0) + processed;

    await this.updateForwardCrawlState(
      latestTx.signature,
      latestTx.slot || 0,
      'completed',
      totalProcessed
    );

    logger.info(
      `Forward crawl processed ${processed} new transactions (cumulative: ${totalProcessed}).`
    );
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

  /**
   * Sleep helper
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

