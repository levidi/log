const { MongoClient } = require("mongodb");
const axios = require("axios");

// IMPORTAÇÃO DO SIMULADOR DE LOGS
require("./create-log");

const MONGO_URI = process.env.MONGO_URI || "mongodb://userflow:passwordflow@mongo:27017/cdcdb?authSource=admin&replicaSet=rs0";
const MONGO_DB = process.env.MONGO_DB || "cdcdb";
const LOKI_URL = process.env.LOKI_URL || "http://loki:3100/loki/api/v1/push";

let buffer = [];
let bufferSize = 0;
const MAX_BUFFER_SIZE = 100;
const FLUSH_INTERVAL_MS = 5000;

function getNanoTimestamp(date) {
  return `${date.getTime()}000000`; // milissegundos para nanosegundos
}

async function sendLogsToLoki(logs) {
  if (logs.length === 0) return;

  const streams = [
    {
      stream: { job: "cdc-app", source: "cdc" },
      values: logs.map(log => [
        getNanoTimestamp(new Date(log.timestamp)),
        JSON.stringify(log)
      ])
    },
  ];

  try {
    await axios.post(LOKI_URL, { streams });
    console.log(`Sent ${logs.length} logs to Loki`);
  } catch (error) {
    console.error("Error sending logs to Loki:", error.message || error);
  }
}

async function flushBuffer() {
  if (bufferSize === 0) return;

  const toSend = buffer;
  buffer = [];
  bufferSize = 0;

  await sendLogsToLoki(toSend);
}

async function main() {
  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log("Connected successfully to MongoDB");

    const db = client.db(MONGO_DB);
    const collection = db.collection("events");

    const pipeline = [{ $match: { operationType: "insert" } }];
    const changeStream = collection.watch(pipeline);

    changeStream.on("change", async (change) => {
      const doc = change.fullDocument;
      console.log("Detected insert:", doc);

      buffer.push({
        _id: doc._id.toString(),
        user: doc.user || "",
        timestamp: doc.timestamp || new Date().toISOString(),

      });
      bufferSize++;

      if (bufferSize >= MAX_BUFFER_SIZE) {
        await flushBuffer();
      }
    });

    setInterval(flushBuffer, FLUSH_INTERVAL_MS);

    process.on("SIGINT", async () => {
      console.log("SIGINT received. Flushing logs and closing Mongo client...");
      await flushBuffer();
      await client.close();
      process.exit(0);
    });

  } catch (err) {
    console.error("Error during operation:", err);
    process.exit(1);
  }
}

main();
