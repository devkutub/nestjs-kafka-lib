import { Kafka, Message, Partitioners, Producer } from 'kafkajs';
import { Logger } from '@nestjs/common';
import { sleep } from './utils/sleep';
import { IProducer } from './interface/producer.interface';

export class KafkajsProducer implements IProducer {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly logger: Logger;

  constructor(
    private readonly topic: string,
    broker: string,
    clientId: string,
  ) {
    this.kafka = new Kafka({ brokers: [broker], clientId });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    this.logger = new Logger(topic);
  }

  async produce(message: Message) {
    await this.producer.send({ topic: this.topic, messages: [message] });
  }

  async connect() {
    try {
      await this.producer.connect();
    } catch (error) {
      this.logger.error('Failed to connect to Kafka server', error);
      await sleep(5000);
      await this.connect();
    }
  }

  async disconnect() {
    await this.producer.disconnect();
  }
}
