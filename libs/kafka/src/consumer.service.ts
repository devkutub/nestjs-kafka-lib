import {
  Injectable,
  OnApplicationShutdown,
  //   OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  Consumer,
  ConsumerRunConfig,
  //   ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  Kafka,
} from 'kafkajs';

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
  constructor(private configService: ConfigService) {}

  // connect to kafka server
  private readonly kafka = new Kafka({
    brokers: [this.configService.get<string>('KAFKA_BROKER')],
  });

  private readonly consumer: Consumer[] = [];

  //   async onModuleInit() {
  //     const consumer = this.kafka.consumer({ groupId: 'test-group' });
  //     // const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka-consumer' });
  //     await consumer.connect();
  //     await consumer.subscribe({ topic: 'election' });
  //     await consumer.run({
  //       eachMessage: async ({ topic, partition, message }) => {
  //         const messageObj = {
  //           value: message.value.toString(),
  //           topic: topic.toString(),
  //           partition: partition.toString(),
  //         };
  //         console.log(messageObj);
  //         // messages.push(messageObj);
  //       },
  //     });
  //   }

  async consume(
    topic: ConsumerSubscribeTopics,
    groupId: string,
    config: ConsumerRunConfig,
  ) {
    const consumer = this.kafka.consumer({ groupId });
    // const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka-consumer' });
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
