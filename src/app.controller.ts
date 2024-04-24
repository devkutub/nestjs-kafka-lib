import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }

  @Post('kafka/produce/:topicName')
  async produceKafkaMessage(
    @Param('topicName') topicName: string,
    @Body() body: { message: string },
  ) {
    return await this.appService.produceKafkaMessage(topicName, body.message);
  }

  @Get('kafka/consume/:topicName')
  async consumeKafkaMessage(
    @Param('topicName') topicName: string,
    @Body() body: { groupId: string },
  ) {
    return await this.appService.consumeKafkaMessage(topicName, body.groupId);
  }
}
