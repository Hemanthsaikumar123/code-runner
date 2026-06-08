require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const { createClient } = require("redis");
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

    app.listen(3000, () => {
      console.log("Server running on port 3000");
    });
  } catch (err) {
    console.error("Startup Error:", err);
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
      INSERT INTO submissions(language, code)
      VALUES($1, $2)
      RETURNING *
      `,
      [language, code]
    );

    res.status(201).json(result.rows[0]);

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

