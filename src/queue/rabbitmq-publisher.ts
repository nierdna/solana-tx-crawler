import amqp from 'amqplib';
import { logger } from '../utils/logger';
import { TransactionMessage } from './types';

export class RabbitMQPublisher {
  private connection: amqp.ChannelModel | null = null;
  private channel: amqp.Channel | null = null;
  private readonly EXCHANGE = 'ore-transactions';
  private readonly QUEUE = 'transaction-etl-v3';
  private readonly DLQ = 'transaction-etl-dlq';
  private readonly ROUTING_KEY = 'new-transaction';
  private isConnected = false;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(private readonly rabbitmqUrl: string) {}

  async connect(): Promise<void> {
    try {
      const connection = await amqp.connect(this.rabbitmqUrl);
      const channel = await connection.createChannel();
      
      this.connection = connection;
      this.channel = channel;

      // Handle connection errors
      connection.on('error', (err) => {
        logger.error('RabbitMQ connection error', err);
        this.handleDisconnect();
      });

      connection.on('close', () => {
        logger.warn('RabbitMQ connection closed');
        this.handleDisconnect();
      });

      // Declare exchange (direct type for routing)
      await channel.assertExchange(this.EXCHANGE, 'direct', {
        durable: true,
      });

      // Setup Dead Letter Queue
      await channel.assertQueue(this.DLQ, {
        durable: true,
      });

      // Declare queue with persistence, TTL and DLQ
      await channel.assertQueue(this.QUEUE, {
        durable: true,
        deadLetterExchange: '',
        deadLetterRoutingKey: this.DLQ,
        // messageTtl: 86400000, // 24 hours
        maxLength: 100000, // Max 100k messages in queue
      });

      // Bind queue to exchange
      await channel.bindQueue(this.QUEUE, this.EXCHANGE, this.ROUTING_KEY);

      this.isConnected = true;
      logger.info('RabbitMQ publisher connected successfully');
    } catch (error) {
      logger.error('Failed to connect to RabbitMQ', error);
      this.handleDisconnect();
      throw error;
    }
  }

  private handleDisconnect(): void {
    this.isConnected = false;
    this.channel = null;
    this.connection = null;

    // Attempt to reconnect after 5 seconds
    if (!this.reconnectTimer) {
      this.reconnectTimer = setTimeout(() => {
        logger.info('RabbitMQ attempting to reconnect...');
        this.reconnectTimer = null;
        this.connect().catch(console.error);
      }, 5000);
    }
  }

  async publishTransaction(transaction: TransactionMessage): Promise<boolean> {
    if (!this.isConnected || !this.channel) {
      logger.warn('RabbitMQ not connected, skipping publish');
      return false;
    }

    try {
      const message = JSON.stringify(transaction);
      const published = this.channel.publish(
        this.EXCHANGE,
        this.ROUTING_KEY,
        Buffer.from(message),
        {
          persistent: true,
          contentType: 'application/json',
          timestamp: Date.now(),
        }
      );

      if (!published) {
        logger.warn('RabbitMQ message buffer full, waiting...');
        await new Promise((resolve) => this.channel!.once('drain', resolve));
      }

      return true;
    } catch (error) {
      logger.error('Failed to publish transaction to RabbitMQ', error);
      return false;
    }
  }

  async publishBatch(transactions: TransactionMessage[]): Promise<void> {
    if (!this.isConnected || !this.channel) {
      logger.warn('RabbitMQ not connected, skipping batch publish');
      return;
    }

    const results = await Promise.allSettled(
      transactions.map((tx) => this.publishTransaction(tx))
    );

    const successful = results.filter((r) => r.status === 'fulfilled' && r.value).length;
    const failed = results.length - successful;

    if (failed > 0) {
      logger.warn(`RabbitMQ batch publish: ${successful} success, ${failed} failed`);
    } else {
      logger.info(`RabbitMQ batch publish: ${successful} transactions published`);
    }
  }

  async disconnect(): Promise<void> {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    try {
      await this.channel?.close();
      await this.connection?.close();
      this.isConnected = false;
      logger.info('RabbitMQ publisher disconnected');
    } catch (error) {
      logger.error('Error during RabbitMQ disconnect', error);
    }
  }
}

// Singleton instance
let publisherInstance: RabbitMQPublisher | null = null;

export function getPublisher(): RabbitMQPublisher {
  if (!publisherInstance) {
    const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
    publisherInstance = new RabbitMQPublisher(rabbitmqUrl);
  }
  return publisherInstance;
}

export async function initializePublisher(): Promise<void> {
  const publisher = getPublisher();
  await publisher.connect();
}

