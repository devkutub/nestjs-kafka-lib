import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaModule } from '@app/kafka';

@Module({
  imports: [KafkaModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
