import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConsumerService } from './consumer.service';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class TestConsumer implements OnModuleInit {
  constructor(
    private readonly consumerService: ConsumerService,
    private readonly configService: ConfigService,
  ) {}

  async onModuleInit() {
    await this.consumerService.consume({
      topic: { topics: [this.configService.get<string>('KAFKA_TOPIC')] },
      config: { groupId: this.configService.get<string>('KAFKA_GROUP_ID') },
      onMessage: async (message) => {
        console.log({
          value: message.value.toString(),
        });
      },
    });
  }
}
