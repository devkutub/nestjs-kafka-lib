import { Body, Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }

  // if you want to produce message for different topic change the code accordingly
  @Post('kafka/produce')
  async produceKafkaMessage(@Body() body: { message: string }) {
    return await this.appService.produceKafkaMessage(body.message);
  }

  // @Get('kafka/consume/:topicName')
  // async consumeKafkaMessage(
  //   @Param('topicName') topicName: string,
  //   @Body() body: { groupId: string },
  // ) {
  //   return await this.appService.consumeKafkaMessage(topicName, body.groupId);
  // }
}
