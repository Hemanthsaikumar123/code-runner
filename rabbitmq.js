const amqp = require("amqplib");

let channel;

async function connectRabbitMQ() {

  while (true) {

    try {

      const connection =
        await amqp.connect("amqp://rabbitmq");

      channel =
        await connection.createChannel();

      await channel.assertQueue(
        "submissions"
      );

      console.log(
        "RabbitMQ Connected"
      );

      return;

    } catch (err) {

      console.log(
        "RabbitMQ not ready. Retrying in 5 seconds..."
      );

      await new Promise(resolve =>
        setTimeout(resolve, 5000)
      );
    }
  }
}

function getChannel() {
  return channel;
}

module.exports = {
  connectRabbitMQ,
  getChannel
};