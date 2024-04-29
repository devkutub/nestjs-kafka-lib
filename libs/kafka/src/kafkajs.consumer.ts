import {
  Consumer,
  ConsumerConfig,
  ConsumerSubscribeTopics,
  Kafka,
  KafkaMessage,
} from 'kafkajs';
import { IConsumer } from './interface/consumer.interface';
import { Logger } from '@nestjs/common';
import { sleep } from './utils/sleep';
import * as retry from 'async-retry';

export class KafkajsConsumer implements IConsumer {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly logger: Logger;

  constructor(
    private readonly topic: ConsumerSubscribeTopics,
    config: ConsumerConfig,
    broker: string,
    clientId: string,
  ) {
    this.kafka = new Kafka({ brokers: [broker], clientId });
    this.consumer = this.kafka.consumer(config);
    this.logger = new Logger(`${topic.topics}-${config.groupId}`);
  }

  async connect() {
    try {
      await this.consumer.connect();
    } catch (error) {
      this.logger.error('Failed to connect to Kafka server', error);
      await sleep(5000);
      await this.connect();
    }
  }

  async consume(onMessage: (message: KafkaMessage) => Promise<void>) {
    await this.consumer.subscribe(this.topic);
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        this.logger.debug(`Processing message topic: ${topic}`);
        this.logger.debug(`Processing message partition: ${partition}`);
        try {
          await retry(async () => onMessage(message), {
            retries: 3,
            onRetry: (error, attempt) =>
              this.logger.error(
                `Error consuming message, executing retry ${attempt}/3...`,
                error,
              ),
          });
        } catch (err) {
          this.logger.error(
            'Error consuming message. Adding to dead letter queue...',
            err,
          );
          // add logic to store the message to somewhere
        }
      },
    });
  }

  async disconnect() {
    await this.consumer.disconnect();
  }
}
