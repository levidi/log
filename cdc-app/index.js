const { MongoClient } = require("mongodb");
const AWS = require("aws-sdk");
const parquet = require("parquetjs-lite");
const fs = require("fs");

const MONGO_URI = process.env.MONGO_URI || "mongodb://userflow:passwordflow@mongo:27017/cdcdb?authSource=admin&replicaSet=rs0";
const MONGO_DB = process.env.MONGO_DB || "cdcdb";
const BUCKET_NAME = "logs";

AWS.config.update({
  accessKeyId: process.env.MINIO_ACCESS_KEY,
  secretAccessKey: process.env.MINIO_SECRET_KEY,
  endpoint: process.env.MINIO_ENDPOINT,
  s3ForcePathStyle: true,
  signatureVersion: "v4",
  region: process.env.REGION,
});

const s3 = new AWS.S3();

let buffer = [];
let fileCounter = 0;

async function uploadBufferIfNeeded() {
  if (buffer.length === 0) return;

  const schema = new parquet.ParquetSchema({
    _id: { type: "UTF8" },
    name: { type: "UTF8" },
    timestamp: { type: "TIMESTAMP_MILLIS" },
  });

  const filePath = `/tmp/cdc-logs-${fileCounter}.parquet`;
  const writer = await parquet.ParquetWriter.openFile(schema, filePath);

  for (const row of buffer) {
    await writer.appendRow(row);
  }

  await writer.close();

  const fileContent = fs.readFileSync(filePath);
  const objectName = `cdc-logs-${new Date().toISOString()}.parquet`;

  await s3.putObject({
    Bucket: BUCKET_NAME,
    Key: objectName,
    Body: fileContent,
  }).promise();

  console.log(`Parquet file uploaded to MinIO as ${objectName}`);

  fs.unlinkSync(filePath);
  buffer = [];
  fileCounter++;
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
        name: doc.user || "",
        timestamp: new Date(doc.timestamp),
      });
    });

    // Checa a cada 5 segundos se há dados para enviar
    setInterval(uploadBufferIfNeeded, 5000);

    process.on("SIGINT", async () => {
      console.log("SIGINT received. Cleaning up...");
      await uploadBufferIfNeeded(); // envia o que restou
      await client.close();
      process.exit(0);
    });

  } catch (err) {
    console.error("Error during operation:", err);
    process.exit(1);
  }
}

main();
