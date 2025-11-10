import { MongoClient, Db, Collection } from 'mongodb';
import { config } from '../config';
import { logger } from '../utils/logger';
import { Transaction, CrawlState, COLLECTIONS } from './schemas';

export class MongoManager {
  private client: MongoClient;
  private db: Db | null = null;
  private isConnected: boolean = false;

  constructor() {
    this.client = new MongoClient(config.mongodb.uri);
  }

  async connect(): Promise<void> {
    try {
      await this.client.connect();
      this.db = this.client.db(config.mongodb.database);
      this.isConnected = true;
      logger.info(`Connected to MongoDB: ${config.mongodb.database}`);
      
      // Create indexes
      await this.createIndexes();
    } catch (error) {
      logger.error('Failed to connect to MongoDB', error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.client.close();
      this.isConnected = false;
      logger.info('Disconnected from MongoDB');
    }
  }

  private async createIndexes(): Promise<void> {
    if (!this.db) throw new Error('Database not connected');

    const transactionsCollection = this.db.collection<Transaction>(COLLECTIONS.TRANSACTIONS);
    await transactionsCollection.createIndex({ signature: 1 }, { unique: true });
    await transactionsCollection.createIndex({ slot: -1 });
    await transactionsCollection.createIndex({ blockTime: -1 });
    logger.info('Created indexes for transactions collection');

    const crawlStateCollection = this.db.collection<CrawlState>(COLLECTIONS.CRAWL_STATE);
    await crawlStateCollection.createIndex({ type: 1 }, { unique: true });
    logger.info('Created indexes for crawl_state collection');
  }

  getTransactionsCollection(): Collection<Transaction> {
    if (!this.db) throw new Error('Database not connected');
    return this.db.collection<Transaction>(COLLECTIONS.TRANSACTIONS);
  }

  getCrawlStateCollection(): Collection<CrawlState> {
    if (!this.db) throw new Error('Database not connected');
    return this.db.collection<CrawlState>(COLLECTIONS.CRAWL_STATE);
  }

  async saveTransaction(transaction: Transaction): Promise<void> {
    const collection = this.getTransactionsCollection();
    await collection.updateOne(
      { signature: transaction.signature },
      { $set: transaction },
      { upsert: true }
    );
  }

  async saveTransactionsBatch(transactions: Transaction[]): Promise<void> {
    if (transactions.length === 0) return;
    
    const collection = this.getTransactionsCollection();
    const bulkOps = transactions.map(tx => ({
      updateOne: {
        filter: { signature: tx.signature },
        update: { $set: tx },
        upsert: true,
      },
    }));
    
    await collection.bulkWrite(bulkOps, { ordered: false });
  }

  async getCrawlState(type: 'backfill' | 'forward'): Promise<CrawlState | null> {
    const collection = this.getCrawlStateCollection();
    return await collection.findOne({ type });
  }

  async updateCrawlState(state: CrawlState): Promise<void> {
    const collection = this.getCrawlStateCollection();
    state.updatedAt = new Date();
    await collection.updateOne(
      { type: state.type },
      { $set: state },
      { upsert: true }
    );
  }

  async getLatestTransaction(): Promise<Transaction | null> {
    const collection = this.getTransactionsCollection();
    const transactions = await collection
      .find({})
      .sort({ slot: -1 })
      .limit(1)
      .toArray();
    return transactions[0] || null;
  }

  async getOldestTransaction(): Promise<Transaction | null> {
    const collection = this.getTransactionsCollection();
    const transactions = await collection
      .find({})
      .sort({ slot: 1 })
      .limit(1)
      .toArray();
    return transactions[0] || null;
  }

  async getTransactionCount(): Promise<number> {
    const collection = this.getTransactionsCollection();
    return await collection.countDocuments({});
  }
}

