const amqp = require("amqplib");

let channel;

async function connectRabbitMQ() {
  const connection = await amqp.connect("amqp://rabbitmq");

  channel = await connection.createChannel();

  await channel.assertQueue("submissions");

  console.log("RabbitMQ Connected");
}

function getChannel() {
  return channel;
}

module.exports = {
  connectRabbitMQ,
  getChannel
};