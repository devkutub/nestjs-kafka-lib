import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IProducer } from './interface/producer.interface';
import { KafkajsProducer } from './kafkajs.producer';
import { Message } from 'kafkajs';

@Injectable()
export class ProducerService implements OnApplicationShutdown {
  private readonly producers = new Map<string, IProducer>();

  constructor(private configService: ConfigService) {}

  async produce(topic: string, message: Message) {
    const producer = await this.getProducer(topic);
    await producer.produce(message);
  }

  async getProducer(topic: string) {
    let producer = this.producers.get(topic);
    if (!producer) {
      producer = new KafkajsProducer(
        topic,
        this.configService.get<string>('KAFKA_BROKER'),
        this.configService.get<string>('KAFKA_CLIENT_ID'),
      );
      await producer.connect();
      this.producers.set(topic, producer);
    }

    return producer;
  }

  async onApplicationShutdown() {
    for (const producer of this.producers.values()) {
      await producer.disconnect();
    }
  }
}
