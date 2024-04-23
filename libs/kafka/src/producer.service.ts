import { Injectable } from '@nestjs/common';
import { Kafka, ProducerRecord } from 'kafkajs';

@Injectable()
export class ProducerService {
  // connect to kafka server
  private readonly kafka = new Kafka({
    brokers: ['localhost:9092'],
  });

  private readonly producer = this.kafka.producer();

  async onModuleInit() {
    await this.producer.connect();
  }

  async produce(record: ProducerRecord) {
    await this.producer.send(record);
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
  }
}
