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
    const workerId =
        Math.random()
          .toString(36)
          .substring(2, 8);

    // console.log("Worker waiting for messages...");

    console.log(
              `Worker ${workerId} started`
            );
    channel.consume("submissions", async (msg) => {

      if (!msg) return;

      try {

        const data =
          JSON.parse(msg.content.toString());

        const submissionId =
          data.submissionId;

        // console.log(
        //   `Processing submission ${submissionId}`
        // );
        console.log(
        `Worker ${workerId} processing submission ${submissionId}`
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

        // const filename =
        //   `submission-${submissionId}.py`;

        // await fs.writeFile(
        //   filename,
        //   submission.code
        // );

        // const filename = `/sandbox/submission-${submissionId}.py`;
        let extension;

        switch (submission.language) {
          case "python":
            extension = "py";
            break;

          case "javascript":
            extension = "js";
            break;

          default:
            throw new Error(
              "Unsupported language"
            );
        }

        const filename =
          `/sandbox/submission-${submissionId}.${extension}`;

        await fs.writeFile(
                filename,
                submission.code
              );
        
        console.log("Created:", filename);

        const files = await fs.readdir("/sandbox");

        // console.log("Sandbox files:", files);

        let output = "";

        try {

          // const { stdout, stderr } =
          //   await execPromise(
          //     `python3 ${filename}`,
          //     {
          //       timeout: 5000
          //     }
          //   );

          // console.log(
          //   "Running sandbox for:",
          //   filename
          // );

          let image;
          let command;

          switch (submission.language) {

            case "python":

              image =
                "python:3.12-alpine";

              command =
                `python ${filename}`;

              break;

            case "javascript":

              image =
                "node:22-alpine";

              command =
                `node ${filename}`;

              break;

            default:

              throw new Error(
                "Unsupported language"
              );
}

          const { stdout, stderr } =
            await execPromise(
              `docker run --rm \
              --memory=128m \
              --cpus=0.5 \
              --network=none \
              -v ${process.env.EXECUTION_VOLUME}:/sandbox \
              ${image} \
              ${command}`,
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