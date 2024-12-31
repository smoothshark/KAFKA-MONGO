from pymongo import MongoClient
from kafka import KafkaProducer, KafkaAdminClient, KafkaException
from kafka.admin import NewTopic
import json
from urllib.parse import quote_plus
import logging

# Configuration des logs pour debug
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration MongoDB
username = "admin"
password = "@Ipadpro8@" 
encoded_password = quote_plus(password)  # Encode le mot de passe pour l'URI MongoDB
mongo_uri = f"mongodb://{username}:{encoded_password}@localhost:27017"  # URI encodé
mongo_db = "meteoDB"  # Nom de la base de données MongoDB
mongo_collection = "testCollection"  # Nom de la collection MongoDB : la collection test

# Configuration Kafka
kafka_bootstrap_servers = "100.78.197.35:9092"  # Adresse du serveur Kafka, mettre localhost si ca ne marche pas
kafka_topic = "OLD.METEOFRANCE.station.2004002"  # Nom du topic Kafka

# Vérification de la connexion MongoDB
try:
    logging.info("Connexion à MongoDB...")
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    logging.info("Connexion à MongoDB réussie.")
except Exception as e:
    logging.error(f"Erreur lors de la connexion à MongoDB : {e}")
    exit(1)

# Vérification de la connexion Kafka et création du topic si nécessaire
try:
    logging.info("Connexion à Kafka...")
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    
    # Vérifie si le topic existe
    existing_topics = admin_client.list_topics()
    if kafka_topic not in existing_topics:
        logging.info(f"Le topic '{kafka_topic}' n'existe pas. Création du topic...")
        new_topic = NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logging.info(f"Topic '{kafka_topic}' créé avec succès.")
    else:
        logging.info(f"Le topic '{kafka_topic}' existe déjà.")

    # Initialisation du producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Sérialisation JSON
        request_timeout_ms=20000,  # Timeout en millisecondes (20 secondes)
        retries=5  # Nombre de tentatives en cas d'échec
    )
    logging.info("Connexion à Kafka réussie.")
except KafkaException as e:
    logging.error(f"Erreur Kafka : {e}")
    client.close()
    exit(1)
except Exception as e:
    logging.error(f"Erreur lors de la connexion à Kafka : {e}")
    client.close()
    exit(1)

# Transférer les documents de MongoDB à Kafka
try:
    logging.info("Début du transfert des données de MongoDB vers Kafka...")
    document_count = 0  # Compteur pour les documents

    for document in collection.find():
        try:
            document_count += 1
            document['_id'] = str(document['_id'])  # Convertir ObjectId en chaîne
            producer.send(kafka_topic, value=document)  # Envoyer le document à Kafka
            logging.info(f"Message envoyé à Kafka : {document}")
        except Exception as e:
            logging.error(f"Erreur lors de l'envoi du document : {e}")

    if document_count == 0:
        logging.warning("Aucun document trouvé dans la collection MongoDB.")

    logging.info("Transfert terminé.")
except Exception as e:
    logging.error(f"Erreur lors du transfert des données : {e}")
finally:
    # Fermer les connexions
    logging.info("Fermeture des connexions...")
    producer.close()
    client.close()