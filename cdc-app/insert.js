const { MongoClient } = require('mongodb');

async function run() {
  const uri = process.env.MONGO_URI
  const dbName = process.env.MONGO_DB

  console.log(`Connecting to MongoDB at ${uri}...`);

  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected successfully to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection('events');

    const now = new Date();

    const doc = {
      user: 'levi',
      action: 'login',
      timestamp: now.toISOString(),
      ip: '192.168.0.1'
    };

    console.log('Inserting document:', doc);
    const result = await collection.insertOne(doc);
    console.log(`Inserted document with _id: ${result.insertedId}`);
  } catch (err) {
    console.error('Error during MongoDB operation:', err);
    process.exit(1);
  } finally {
    await client.close();
    console.log('Connection closed');
  }
}

run().catch((err) => {
  console.error('Unexpected error:', err);
  process.exit(1);
});