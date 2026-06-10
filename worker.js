const amqp = require("amqplib");
const { Pool } = require("pg");
const { exec } = require("child_process");
const util = require("util");
const fs = require("fs/promises");

const execPromise = util.promisify(exec);

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.POSTGRES_PORT,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});


async function connectRabbitMQ() {

  while (true) {

    try {

      const connection =
        await amqp.connect("amqp://rabbitmq");

      console.log(
        "RabbitMQ Connected"
      );

      return connection;

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

async function startWorker() {
  try {

    const connection = await connectRabbitMQ();

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
          WHERE id=$1
          `,
          [submissionId]
        );

        const submission = result.rows[0];

        const filename =
          `submission-${submissionId}.py`;

        await fs.writeFile(
          filename,
          submission.code
        );

        let output = "";

        try {

          const { stdout, stderr } =
            await execPromise(
              `python3 ${filename}`,
              {
                timeout: 5000
              }
            );

          output = stdout || stderr;

          await pool.query(
            `
            UPDATE submissions
            SET status='COMPLETED',
                output=$1
            WHERE id=$2
            `,
            [output, submissionId]
          );

        } catch (executionError) {

          output =
            executionError.stderr ||
            executionError.message;

          await pool.query(
            `
            UPDATE submissions
            SET status='FAILED',
                output=$1
            WHERE id=$2
            `,
            [output, submissionId]
          );
        }

        await fs.unlink(filename);

        channel.ack(msg);

      } catch (err) {

        console.error(
          "Worker Error:",
          err
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