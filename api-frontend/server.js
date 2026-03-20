const express = require("express");
const cors = require("cors");
const path = require("path");
const axios = require("axios");
const { createClient } = require("redis");

const API_PORT = process.env.API_PORT || 8000;
const API_HOST = process.env.API_HOST || "0.0.0.0";

const REDIS_HOST = process.env.API_REDIS_HOST || "redis";
const REDIS_PORT = process.env.API_REDIS_PORT || 6379;
const REDIS_CHANNEL = process.env.API_REDIS_CHANNEL || "cdc_events";

const MEILI_HOST = process.env.MEILI_HOST || "http://meilisearch:7700";
const MEILI_INDEX_NAME = process.env.MEILI_INDEX_NAME || "products";
const MEILI_MASTER_KEY =
  process.env.MEILI_MASTER_KEY || "master_key_placeholder";

const app = express();
app.use(cors());
app.use(express.json());

// Serve static frontend
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// Search API: proxies to Meilisearch
app.get("/api/search", async (req, res) => {
  try {
    const q = req.query.q || "";
    const category = req.query.category || "";
    const inStock = req.query.inStock || "";
    const filters = [];

    if (category) filters.push(`category = "${category}"`);
    if (inStock === "true") filters.push("in_stock = true");

    const payload = {
      q,
      filter: filters.length ? filters.join(" AND ") : null,
    };

    const response = await axios.post(
      `${MEILI_HOST}/indexes/${MEILI_INDEX_NAME}/search`,
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${MEILI_MASTER_KEY}`,
        },
      }
    );

    res.json(response.data);
  } catch (err) {
    console.error("Search error:", err.message);
    res.status(500).json({ error: "Search failed" });
  }
});

// SIMPLE TICK-BASED SSE (works independently of Redis)
app.get("/api/cdc-stream", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  let counter = 0;

  const interval = setInterval(() => {
    const payload = {
      table: "products",
      operation: "TICK",
      timestamp: new Date().toISOString(),
      counter: counter++,
    };

    // IMPORTANT: real newlines
    res.write("event: cdc_event\n");
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
  }, 2000);

  req.on("close", () => {
    clearInterval(interval);
  });
});

// Root: serve HTML
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(API_PORT, API_HOST, () => {
  console.log(`API/Frontend listening on ${API_HOST}:${API_PORT}`);
});
