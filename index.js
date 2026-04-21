const amqp = require("amqplib/callback_api");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const RABBITMQ_CONNECTION_STRING =
  process.env.RABBITMQ_CONNECTION_STRING || "amqp://localhost";
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || "bestbuy";
const QUEUE_NAME = process.env.QUEUE_NAME || "order_queue";

const client = new MongoClient(MONGO_URI);

let ordersCollection;
let productsCollection;

async function connectToMongo() {
  try {
    await client.connect();

    const db = client.db(DB_NAME);
    ordersCollection = db.collection("orders");
    productsCollection = db.collection("products");

    console.log("Connected to MongoDB");
  } catch (err) {
    console.error("MongoDB connection error:", err);
    process.exit(1);
  }
}

async function processOrder(order) {
  try {
    if (!ordersCollection || !productsCollection) {
      throw new Error("MongoDB collections not initialized");
    }

    const items = order.items || [];
    let canFulfill = true;

    for (const item of items) {
      const product = await productsCollection.findOne({ id: item.productId });

      if (!product || product.stock < item.quantity) {
        canFulfill = false;
        break;
      }
    }

    if (!canFulfill) {
      await ordersCollection.updateOne(
        {
          customerName: order.customerName,
          total: order.total,
          createdAt: new Date(order.createdAt)
        },
        { $set: { status: "rejected" } }
      );

      console.log(`Order rejected for ${order.customerName}`);
      return;
    }

    for (const item of items) {
      const result = await productsCollection.updateOne(
        { id: item.productId, stock: { $gte: item.quantity } },
        { $inc: { stock: -item.quantity } }
      );

      if (result.modifiedCount === 0) {
        await ordersCollection.updateOne(
          {
            customerName: order.customerName,
            total: order.total,
            createdAt: new Date(order.createdAt)
          },
          { $set: { status: "rejected" } }
        );

        console.log(`Order rejected during stock update for ${order.customerName}`);
        return;
      }
    }

    await ordersCollection.updateOne(
      {
        customerName: order.customerName,
        total: order.total,
        createdAt: new Date(order.createdAt)
      },
      { $set: { status: "completed" } }
    );

    console.log(`Order completed for ${order.customerName}`);
  } catch (err) {
    console.error("Error processing order:", err);
  }
}

async function startConsumer() {
  await connectToMongo();

  amqp.connect(RABBITMQ_CONNECTION_STRING, (err, conn) => {
    if (err) {
      console.error("RabbitMQ connection error:", err);
      process.exit(1);
    }

    conn.createChannel((err, channel) => {
      if (err) {
        console.error("RabbitMQ channel error:", err);
        process.exit(1);
      }

      channel.assertQueue(QUEUE_NAME, { durable: true });
      console.log(`Waiting for messages in ${QUEUE_NAME}...`);

      channel.consume(
        QUEUE_NAME,
        async (msg) => {
          if (msg !== null) {
            const order = JSON.parse(msg.content.toString());
            console.log("Received order:", order);

            await processOrder(order);

            channel.ack(msg);
          }
        },
        { noAck: false }
      );
    });
  });
}

startConsumer();