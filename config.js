import { Kafka } from "kafkajs";

class kafkaConfig {

  constructor(){
    this.kafka = new Kafka({
      clientId: "nodejs-kafka",
      brokers: ['localhost:9093']
    });
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({groupId: "test-group"});
  }

  async produce(topic, message){
    try{


      await this.producer.connect()
      await this.producer.send({
        topic,
        messages: [{value: message}],
      });
    }catch(err){
      console.log(err)
    } finally {
      await this.producer.disconnect()
    }
  }

  async consume(topic, callback){
    try{
      await this.consumer.connect();
      await this.consumer.subscribe({topic: topic, fromBeginning:true});
      await this.consumer.run({
        eachMessage: async ({topic,partiion, message}) =>{
          const value = message.value.toString()
          callback(value)
        }
      });
    }catch(err){
      console.log(err)
    }
  }
}

export default kafkaConfig;