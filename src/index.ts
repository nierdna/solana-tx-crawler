import { SolanaClient } from './services/solana-client';
import { MongoManager } from './database/mongo';
import { BackfillService } from './services/backfill';
import { ForwardCrawler } from './services/forward-crawler';
import { logger } from './utils/logger';
import { config } from './config';
import { initializePublisher, getPublisher } from './queue/rabbitmq-publisher';

class SolanaCrawlerApp {
  private mongoManager: MongoManager;
  private solanaClient: SolanaClient;
  private backfillService: BackfillService;
  private forwardCrawler: ForwardCrawler;
  private isShuttingDown: boolean = false;

  constructor() {
    this.mongoManager = new MongoManager();
    this.solanaClient = new SolanaClient();
    this.backfillService = new BackfillService(this.solanaClient, this.mongoManager);
    this.forwardCrawler = new ForwardCrawler(this.solanaClient, this.mongoManager);
  }

  /**
   * Initialize the application
   */
  async initialize(): Promise<void> {
    logger.info('Initializing Solana Transaction Crawler...');
    logger.info(`Program Address: ${config.solana.programAddress}`);
    logger.info(`RPC URL: ${config.solana.rpcUrl}`);
    logger.info(`MongoDB URI: ${config.mongodb.uri}`);
    logger.info(`Database: ${config.mongodb.database}`);

    // Connect to MongoDB
    await this.mongoManager.connect();

    // Initialize RabbitMQ publisher
    try {
      await initializePublisher();
      logger.info('RabbitMQ publisher initialized');
    } catch (error) {
      logger.warn('Failed to initialize RabbitMQ publisher, will continue without it', error);
    }

    // Setup graceful shutdown handlers
    this.setupShutdownHandlers();

    logger.info('Initialization complete');
  }

  /**
   * Run the crawler
   */
  async run(): Promise<void> {
    try {
      if (config.crawler.skipBackfill) {
        logger.info('SKIP_BACKFILL flag detected, skipping backfill phase.');
      } else {
        // Check backfill status
        const backfillState = await this.mongoManager.getCrawlState('backfill');

        if (!backfillState || backfillState.status === 'in_progress') {
          // Start or resume backfill
          logger.info('=== BACKFILL PHASE ===');
          await this.backfillService.start();

          if (this.isShuttingDown) {
            logger.info('Shutdown requested during backfill');
            return;
          }

          // Show statistics after backfill
          await this.showStatistics();
        } else {
          logger.info('Backfill already completed');
          await this.showStatistics();
        }
      }

      // After backfill is complete, run forward crawler
      logger.info('=== FORWARD CRAWL PHASE ===');
      await this.forwardCrawler.start();
      
      if (this.isShuttingDown) {
        logger.info('Shutdown requested during forward crawl');
        return;
      }

      // Show final statistics
      await this.showStatistics();
      
      logger.info('=== CRAWLING COMPLETE ===');
    } catch (error) {
      logger.error('Error during crawler execution', error);
      throw error;
    }
  }

  /**
   * Show current database statistics
   */
  private async showStatistics(): Promise<void> {
    const totalCount = await this.mongoManager.getTransactionCount();
    const latestTx = await this.mongoManager.getLatestTransaction();
    const oldestTx = await this.mongoManager.getOldestTransaction();

    logger.info('=== DATABASE STATISTICS ===');
    logger.info(`Total transactions: ${totalCount}`);
    
    if (latestTx) {
      logger.info(`Latest transaction: ${latestTx.signature} (slot: ${latestTx.slot})`);
      if (latestTx.blockTime) {
        logger.info(`Latest block time: ${new Date(latestTx.blockTime * 1000).toISOString()}`);
      }
    }
    
    if (oldestTx) {
      logger.info(`Oldest transaction: ${oldestTx.signature} (slot: ${oldestTx.slot})`);
      if (oldestTx.blockTime) {
        logger.info(`Oldest block time: ${new Date(oldestTx.blockTime * 1000).toISOString()}`);
      }
    }
    
    logger.info('===========================');
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupShutdownHandlers(): void {
    const shutdown = async (signal: string) => {
      if (this.isShuttingDown) {
        logger.warn('Shutdown already in progress...');
        return;
      }

      this.isShuttingDown = true;
      logger.info(`\nReceived ${signal}. Gracefully shutting down...`);

      try {
        // Stop services
        this.backfillService.stop();
        this.forwardCrawler.stop();

        // Give services time to save state
        await new Promise(resolve => setTimeout(resolve, 2000));

        // Disconnect from MongoDB
        await this.mongoManager.disconnect();

        logger.info('Shutdown complete');
        process.exit(0);
      } catch (error) {
        logger.error('Error during shutdown', error);
        process.exit(1);
      }
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    
    // Handle uncaught errors
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught exception', error);
      shutdown('uncaughtException');
    });

    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled rejection', { reason, promise });
      shutdown('unhandledRejection');
    });
  }

  /**
   * Cleanup and disconnect
   */
  async cleanup(): Promise<void> {
    // Disconnect RabbitMQ publisher
    try {
      const publisher = getPublisher();
      await publisher.disconnect();
    } catch (error) {
      logger.error('Error disconnecting RabbitMQ publisher', error);
    }

    await this.mongoManager.disconnect();
  }
}

/**
 * Main entry point
 */
async function main() {
  const app = new SolanaCrawlerApp();

  try {
    await app.initialize();
    await app.run();
    await app.cleanup();
  } catch (error) {
    logger.error('Fatal error in main', error);
    process.exit(1);
  }
}

// Run the application
main();

