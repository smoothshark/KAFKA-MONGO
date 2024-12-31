import os
import json
import logging
from time import sleep
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from urllib.parse import quote_plus

# Logging configuration
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB configuration
mongo_username = os.getenv("MONGO_USERNAME", "admin")
mongo_password = os.getenv("MONGO_PASSWORD", "password")
encoded_password = quote_plus(mongo_password)
mongo_host = os.getenv("MONGO_HOST", "localhost:27017")
mongo_db_name = os.getenv("MONGO_DB", "meteoDB")
mongo_collection_name = os.getenv("MONGO_COLLECTION", "meteoCollection")

# Kafka configuration
kafka_bootstrap_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
base_kafka_topic = os.getenv("BASE_KAFKA_TOPIC", "METEOFRANCE.station.")
kafka_retries = int(os.getenv("KAFKA_RETRIES", 3))
retry_interval = int(os.getenv("RETRY_INTERVAL", 5))  # Retry interval in seconds

# Function to ensure Kafka topic existence with retries
def ensure_kafka_topic(admin_client, topic_name, retries, interval):
    for attempt in range(retries):
        try:
            existing_topics = admin_client.list_topics()
            if topic_name not in existing_topics:
                logging.info(f"Topic '{topic_name}' does not exist. Creating...")
                new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                admin_client.create_topics(new_topics=[new_topic], validate_only=False)
                logging.info(f"Topic '{topic_name}' created successfully.")
            else:
                logging.info(f"Topic '{topic_name}' already exists.")
            return
        except Exception as e:
            logging.error(f"Error checking/creating topic '{topic_name}': {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {interval} seconds (Attempt {attempt + 1}/{retries})...")
                sleep(interval)
            else:
                raise e

# MongoDB connection
try:
    logging.info("Connecting to MongoDB...")
    mongo_uri = f"mongodb://{mongo_username}:{encoded_password}@{mongo_host}"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[mongo_db_name]
    collection = db[mongo_collection_name]
    logging.info("Successfully connected to MongoDB.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# Kafka connection
try:
    logging.info("Connecting to Kafka...")
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=20000,
        retries=5
    )
    logging.info("Successfully connected to Kafka.")
except Exception as e:
    logging.error(f"Failed to connect to Kafka: {e}")
    client.close()
    exit(1)

# Transfer MongoDB documents to Kafka
try:
    logging.info("Starting data transfer from MongoDB to Kafka...")
    document_count = 0
    batch_size = int(os.getenv("KAFKA_BATCH_SIZE", 100))
    buffer = []

    for document in collection.find():
        document_count += 1
        document['_id'] = str(document['_id'])  # Convert ObjectId to string

        if 'NUM_POSTE' in document:
            topic_name = f"{base_kafka_topic}{document['NUM_POSTE']}"
            try:
                ensure_kafka_topic(admin_client, topic_name, kafka_retries, retry_interval)
                buffer.append(document)

                if len(buffer) >= batch_size:
                    for doc in buffer:
                        producer.send(topic_name, value=doc)
                    producer.flush()
                    logging.info(f"Batch of {len(buffer)} messages sent to topic '{topic_name}'.")
                    buffer = []

            except Exception as e:
                logging.error(f"Error sending document to topic '{topic_name}': {e}")
        else:
            logging.warning(f"Document ignored (missing 'NUM_POSTE'): {document}")

    # Send remaining buffer
    if buffer:
        for doc in buffer:
            producer.send(topic_name, value=doc)
        producer.flush()
        logging.info(f"Final batch of {len(buffer)} messages sent to topic '{topic_name}'.")

    if document_count == 0:
        logging.warning("No documents found in MongoDB collection.")
    else:
        logging.info(f"Data transfer completed. Total documents processed: {document_count}.")
except Exception as e:
    logging.error(f"Error during data transfer: {e}")
finally:
    logging.info("Closing connections...")
    producer.close()
    client.close()