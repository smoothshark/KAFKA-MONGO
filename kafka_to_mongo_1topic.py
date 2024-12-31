import os
import json
import logging
import time
from pymongo import MongoClient, errors as pymongo_errors
from kafka import KafkaProducer, KafkaAdminClient, KafkaException, errors as kafka_errors
from kafka.admin import NewTopic
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load sensitive information from environment variables (Improvement: Security enhancement)
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASS = os.getenv("MONGO_PASS", "@Ipadpro8@")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = os.getenv("MONGO_DB", "meteoDB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "testCollection")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "100.78.197.35:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "OLD.METEOFRANCE.station.2004002")

# Encode MongoDB password and build URI (Improvement: Prevent special character issues in credentials)
encoded_password = quote_plus(MONGO_PASS)
mongo_uri = f"mongodb://{MONGO_USER}:{encoded_password}@{MONGO_HOST}:{MONGO_PORT}"

def connect_to_mongo(uri, db_name, collection_name):
    """
    Connect to MongoDB with retry mechanism (Improvement: Retry logic added for robustness).
    """
    for attempt in range(5):  # Retry connection up to 5 times
        try:
            client = MongoClient(uri)
            db = client[db_name]
            collection = db[collection_name]
            logging.info("Connected to MongoDB successfully.")
            return client, collection
        except pymongo_errors.ConnectionFailure as e:
            logging.error(f"Failed to connect to MongoDB, attempt {attempt + 1}: {e}")
            time.sleep(5)  # Wait before retrying
    logging.critical("Failed to connect to MongoDB after multiple attempts. Exiting.")
    exit(1)

def connect_to_kafka(servers, topic_name):
    """
    Connect to Kafka and ensure the topic exists (Improvement: Topic creation logic added).
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=servers)
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            logging.info(f"Kafka topic '{topic_name}' does not exist. Creating...")
            new_topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=2)
            admin_client.create_topics(new_topics=[new_topic])
            logging.info(f"Kafka topic '{topic_name}' created successfully.")
        else:
            logging.info(f"Kafka topic '{topic_name}' already exists.")
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=20000,
            retries=5
        )
        logging.info("Connected to Kafka successfully.")
        return producer
    except kafka_errors.KafkaError as e:
        logging.error(f"Kafka connection failed: {e}")
        exit(1)

def transfer_data(mongo_collection, kafka_producer, kafka_topic, batch_size=100):
    """
    Transfer data from MongoDB to Kafka in batches (Improvement: Batch processing implemented).
    """
    try:
        logging.info("Starting data transfer from MongoDB to Kafka...")
        batch = []
        document_count = 0
        # Use cursor with batch_size to optimize MongoDB read
        for document in mongo_collection.find({}, no_cursor_timeout=True).batch_size(batch_size):
            document['_id'] = str(document['_id'])  # Convert ObjectId to string for JSON serialization
            batch.append(document)
            if len(batch) >= batch_size:
                kafka_producer.send(kafka_topic, value=batch)
                kafka_producer.flush()  # Ensure all messages are sent
                logging.info(f"Sent batch of {len(batch)} messages to Kafka.")
                batch = []  # Reset the batch
                document_count += len(batch)
        if batch:  # Send any remaining documents
            kafka_producer.send(kafka_topic, value=batch)
            kafka_producer.flush()
            logging.info(f"Sent final batch of {len(batch)} messages to Kafka.")
            document_count += len(batch)
        logging.info(f"Data transfer complete. Total {document_count} messages sent.")
    except Exception as e:
        logging.error(f"Error during data transfer: {e}")
    finally:
        logging.info("Closing MongoDB cursor...")
        mongo_collection.database.client.close()  # Close MongoDB connection

def main():
    """
    Main function: Connects to MongoDB and Kafka, then transfers data.
    """
    # Connect to MongoDB (Improved with retry and error handling)
    mongo_client, mongo_collection = connect_to_mongo(mongo_uri, MONGO_DB, MONGO_COLLECTION)

    # Connect to Kafka (Improved with topic validation/creation logic)
    kafka_producer = connect_to_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    # Transfer data (Improved with batch processing and logging)
    transfer_data(mongo_collection, kafka_producer, KAFKA_TOPIC)

    # Close resources
    logging.info("Closing Kafka producer...")
    kafka_producer.flush()
    kafka_producer.close()
    logging.info("Program completed.")

if __name__ == "__main__":
    main()