import { ObjectId } from 'mongodb';

export interface Transaction {
  _id?: ObjectId;
  signature: string;
  slot: number;
  blockTime: number | null;
  err: any;
  parsedData: any;
  createdAt: Date;
}

export interface CrawlState {
  _id?: ObjectId;
  type: 'backfill' | 'forward';
  lastSignature: string;
  lastSlot: number;
  status: 'in_progress' | 'completed';
  updatedAt: Date;
  totalProcessed?: number;
  backfillStartSignature?: string; // The very first signature when backfill started
}

export const COLLECTIONS = {
  TRANSACTIONS: 'transactions',
  CRAWL_STATE: 'crawl_state',
} as const;

