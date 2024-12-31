import os
import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from urllib.parse import quote_plus

# Logging configuration
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
kafka_bootstrap_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
kafka_topic_pattern = os.getenv("KAFKA_TOPIC_PATTERN", r"^METEOFRANCE\.station\..+")

# MongoDB configuration
mongo_username = os.getenv("MONGO_USERNAME", "admin")
mongo_password = os.getenv("MONGO_PASSWORD", "password")
encoded_password = quote_plus(mongo_password)
mongo_host = os.getenv("MONGO_HOST", "localhost:27017")
mongo_db_name = os.getenv("MONGO_DB", "meteoDB")
mongo_collection_name = os.getenv("MONGO_COLLECTION", "testCollection")

# Connect to MongoDB
try:
    logging.info("Connecting to MongoDB...")
    mongo_uri = f"mongodb://{mongo_username}:{encoded_password}@{mongo_host}"
    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    collection = db[mongo_collection_name]
    logging.info("Successfully connected to MongoDB.")
except Exception as e:
    logging.error(f"Error connecting to MongoDB: {e}")
    exit(1)

# Connect to Kafka
try:
    logging.info("Connecting to Kafka...")
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe(pattern=kafka_topic_pattern)
    logging.info("Successfully connected to Kafka and subscribed to matching topics.")
except Exception as e:
    logging.error(f"Error connecting to Kafka: {e}")
    client.close()
    exit(1)

# Consume Kafka messages and insert into MongoDB
try:
    logging.info("Starting Kafka to MongoDB data consumption...")
    batch_size = int(os.getenv("MONGO_BATCH_SIZE", 100))  # Batch size for MongoDB insertions
    buffer = []  # Temporary storage for batch insertion

    for message in consumer:
        try:
            buffer.append(message.value)
            logging.info(f"Received message from topic {message.topic}: {message.value}")

            # Insert into MongoDB in batches
            if len(buffer) >= batch_size:
                collection.insert_many(buffer)
                logging.info(f"Batch of {batch_size} messages inserted into MongoDB.")
                buffer = []  # Clear the buffer

        except Exception as e:
            logging.error(f"Error processing message: {e}")

    # Insert remaining messages in buffer
    if buffer:
        collection.insert_many(buffer)
        logging.info(f"Final batch of {len(buffer)} messages inserted into MongoDB.")

    logging.info("Data consumption completed.")
except Exception as e:
    logging.error(f"Error during Kafka data consumption: {e}")
finally:
    # Close connections
    logging.info("Closing connections...")
    consumer.close()
    client.close()