import { Module } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { ConsumerService } from './consumer.service';
import { TestConsumer } from './test.consumer';

@Module({
  providers: [ProducerService, ConsumerService, TestConsumer],
  exports: [ProducerService, ConsumerService],
})
export class KafkaModule {}
