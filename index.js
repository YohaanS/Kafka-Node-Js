import bodyParser from "body-parser";
import express from "express";
import controllers from "./controller.js";
import KafkaConfig from "./config.js";

const app = express()
const jsonParser = bodyParser.json()

app.post("/api/send",jsonParser, controllers.sendMessageToKafka)

const kafkaConfig = new KafkaConfig()
kafkaConfig.consume("my-topic",(value)=>{
  console.log(value)
})

app.listen(8080, ()=>(
  console.log("Server is running on port 8080.")
))