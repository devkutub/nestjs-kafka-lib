import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IConsumer } from './interface/consumer.interface';
import { KafkajsConsumerOptions } from './kafkajs-consumer-options.interface';
import { KafkajsConsumer } from './kafkajs.consumer';

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
  constructor(private configService: ConfigService) {}

  private readonly consumers: IConsumer[] = [];

  async consume({ topic, config, onMessage }: KafkajsConsumerOptions) {
    const consumer = new KafkajsConsumer(
      topic,
      config,
      this.configService.get<string>('KAFKA_BROKER'),
      this.configService.get<string>('KAFKA_CLIENT_ID'),
    );
    await consumer.connect();
    await consumer.consume(onMessage);
    this.consumers.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }
}
