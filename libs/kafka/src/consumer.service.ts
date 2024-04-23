import { Injectable } from '@nestjs/common';
import {
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  Kafka,
} from 'kafkajs';

@Injectable()
export class ConsumerService {
  // connect to kafka server
  private readonly kafka = new Kafka({
    brokers: ['localhost:9092'],
  });

  private readonly consumer: Consumer[] = [];

  async consume(topic: ConsumerSubscribeTopic, config: ConsumerRunConfig) {
    const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka-consumer' });
    await consumer.connect();
    await consumer.subscribe(topic);
    await consumer.run(config);

    this.consumer.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumer) {
      await consumer.disconnect();
    }
  }
}
