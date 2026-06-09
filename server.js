require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const { createClient } = require("redis");
const { connectRabbitMQ } = require("./rabbitmq");
const { getChannel } = require("./rabbitmq");

const app = express();
app.use(express.json());


const redisClient = createClient({
  url: "redis://redis:6379",
});

redisClient.on("error", (err) => {
  console.error(err);
});

async function startServer() {
  try {
    await redisClient.connect();

    await connectRabbitMQ();

    app.listen(3000, () => {
      console.log("Server running on port 3000");
    });

  } catch (err) {
    console.error(err);
  }
}

startServer();


const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.POSTGRES_PORT,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});

app.get("/", async (req, res) => {
  res.send("Welcome to Code Runner API");
});

app.post("/submission", async (req, res) => {
  try {
    const { language, code } = req.body;

    const result = await pool.query(
      `
      INSERT INTO submissions(language, code, status)
      VALUES($1, $2, 'QUEUED')
      RETURNING *
      `,
      [language, code]
    );

    const submission = result.rows[0];

    const channel = getChannel();

    channel.sendToQueue(
      "submissions",
      Buffer.from(
        JSON.stringify({
          submissionId: submission.id
        })
      )
    );

    res.status(201).json({
      message: "Submission queued successfully",
      submission
    });

  } catch (err) {
    console.error(err);

    res.status(500).json({
      error: err.message
    });
  }
});
app.get("/submission/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const cacheKey = `submission:${id}`;

    const cached = await redisClient.get(cacheKey);

    if (cached) {
    return res.json(JSON.parse(cached));
    }

    const result = await pool.query(
      `
      SELECT *
      FROM submissions
      WHERE id = $1
      `,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        message: "Submission not found"
      });
    }
    
    await redisClient.set(
    cacheKey,
    JSON.stringify(result.rows[0]),
    {
        EX: 60
    }
    );


    return res.json(result.rows[0]);


  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: err.message
    });
  }
});



app.post("/queue", async (req, res) => {

  const channel = getChannel();

  const message = {
    language: req.body.language,
    code: req.body.code
  };

  channel.sendToQueue(
    "submissions",
    Buffer.from(JSON.stringify(message))
  );

  res.json({
    message: "Submission queued"
  });

});

