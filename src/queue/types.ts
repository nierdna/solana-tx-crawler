export interface TransactionMessage {
  signature: string;
  slot: number;
  blockTime: number | null;
  err: any;
  parsedData: any;
  createdAt: Date;
}

