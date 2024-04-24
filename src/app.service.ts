import { Injectable } from '@nestjs/common';
import { ConsumerService, ProducerService } from '@app/kafka';

@Injectable()
export class AppService {
  constructor(
    private producerService: ProducerService,
    private consumerService: ConsumerService,
  ) {}

  getHello(): string {
    return 'Hello World!';
  }

  async produceKafkaMessage(topic: string, message: string) {
    return await this.producerService.produce({
      topic,
      messages: [{ value: message }],
    });
  }

  async consumeKafkaMessage(topic: string, groupId: string) {
    // const messages = [];

    await this.consumerService.consume(
      { topics: [topic], fromBeginning: true },
      groupId,
      {
        eachMessage: async ({ topic, partition, message }) => {
          const messageObj = {
            value: message.value.toString(),
            topic: topic.toString(),
            partition: partition.toString(),
          };
          console.log(messageObj);
          // messages.push(messageObj);
        },
      },
    );

    // console.log('Consumed messages:', messages);

    return 'messages consumed';
  }
}
