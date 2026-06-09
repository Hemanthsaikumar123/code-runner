const amqp = require("amqplib");
const { Pool } = require("pg");

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.POSTGRES_PORT,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});

async function startWorker() {
  try {

    const connection =
      await amqp.connect("amqp://rabbitmq");

    const channel =
      await connection.createChannel();

    await channel.assertQueue("submissions");

    console.log("Worker waiting for messages...");

    channel.consume("submissions", async (msg) => {

      if (!msg) return;

      try {

        const data =
          JSON.parse(msg.content.toString());

        const submissionId =
          data.submissionId;

        console.log(
          `Processing submission ${submissionId}`
        );

        await pool.query(
          `
          UPDATE submissions
          SET status='PROCESSING'
          WHERE id=$1
          `,
          [submissionId]
        );

        const result = await pool.query(
            `
            SELECT *
            FROM submissions
            WHERE id = $1
            `,
            [submissionId]
            );

        const submission = result.rows[0];

        console.log("Language:", submission.language);
        console.log("Code:", submission.code);

        await pool.query(
          `
          UPDATE submissions
          SET status='COMPLETED'
          WHERE id=$1
          `,
          [submissionId]
        );

        console.log(
          `Completed submission ${submissionId}`
        );

        channel.ack(msg);

      } catch (err) {

        console.error(
          "Worker Error:",
          err
        );

        await pool.query(
          `
          UPDATE submissions
          SET status='FAILED'
          WHERE id=$1
          `,
          [
            JSON.parse(
              msg.content.toString()
            ).submissionId
          ]
        );

        channel.ack(msg);
      }

    });

  } catch (err) {
    console.error(
      "Worker Startup Error:",
      err
    );
  }
}

startWorker();