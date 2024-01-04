import os
import time
import mysql.connector
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient


def delivery_report(err, msg):#It reports the delivery status of the data to kafka cluster
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None: #If some error is found
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        "User record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )#If message was successfully sent to cluster
    )


kafka_config = {
    "bootstrap.servers": "********************",#The kafka cloud bootstrapserver
    "sasl.mechanisms": "PLAIN",#SASL mechanisms for authentication
    "security.protocol": "SASL_SSL",#Security Protocol
    "sasl.username": "************",#SASL USERNAME(API)
    "sasl.password": "********************************",#SASL PASSWORD(API)
}
# Create a Schema Registry client which manages the updated schema
schema_registry_client = SchemaRegistryClient(
    {
        "url": "https://psrc-5j7x8.us-central1.gcp.confluent.cloud",#The schema registry url
        "basic.auth.user.info": "{}:{}".format(
            "*********",#Schema Registry key
            "*********************************************",#Schema registry value/secret
        ),
    }
)

# Fetch the latest Avro schema for the value
subject_name = "product_updates-value"  # topic_name+(-value)
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str#It gives the latest updated schema version

# Create Avro Serializer for the value
key_serializer = StringSerializer("utf_8") # Serializer for keys, encoded as strings
avro_serializer = AvroSerializer(schema_registry_client, schema_str)# Serializer for Avro schema

# Define the SerializingProducer
producer = SerializingProducer(
    {
        "bootstrap.servers": kafka_config["bootstrap.servers"],# Kafka bootstrap servers
        "security.protocol": kafka_config["security.protocol"],# Security protocol configuration
        "sasl.mechanisms": kafka_config["sasl.mechanisms"],#SASL mechanisms for authentication
        "sasl.username": kafka_config["sasl.username"],#SASL USERNAME(API)
        "sasl.password": kafka_config["sasl.password"],#SASL PASSWORD(API)
        "key.serializer": key_serializer,  # Key will be serialized as a string
        "value.serializer": avro_serializer,  # Value will be serialized as Avro
    }
)
#MYSQL Configuration
mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "buy_online",
}

timestamp_file = "last_timestamp.txt"#It stores the timestamp of last inserted record into the the kafka cluster to avoid duplicates
if os.path.exists(timestamp_file):#If the file exists
    with open(timestamp_file, "r") as file:#Open in read mode
        last_read_timestamp = file.read()#Read the timestamp
else:
    last_read_timestamp = None
while True:
    db_connection = mysql.connector.connect(**mysql_config)#Start connection
    cursor = db_connection.cursor()#Cursor for dealing with mysql server
    if last_read_timestamp is None:#If there is no timestamp i.e.intial record
        sql_query = "SELECT product_id, product_name, category, price, updated_timestamp FROM products"
    else:#If timestamp exists
        sql_query = f"SELECT product_id, product_name, category, price, updated_timestamp FROM products WHERE updated_timestamp > '{last_read_timestamp}'"
    cursor.execute(sql_query)#Execute the selected query
    records = cursor.fetchall()#Fetch all the records for the query executed as rows

    if records:  # If records are found
        for row in records:#for every row in records
            if row[4]:  # Check if updated_timestamp is not empty or None
                last_read_timestamp = str(row[4])#store the current record timestamp into variable
                value = {
                    "product_id": int(row[0]),
                    "product_name": row[1],
                    "category": row[2],
                    "price": float(row[3]),
                    "updated_timestamp": str(row[4]),
                }#Store these values into respected columns of avro format
                producer.produce(
                    topic="product_updates", key=str(value["product_id"]), value=value
                )# Push the record into the specified topic with the given key-value pair
                producer.flush()# Flush the producer buffer to ensure all messages are sent
                print(f"Message id_{row[0]} is published")#Print the published message
    if last_read_timestamp:#If there is last_read_timestamp value
        with open(timestamp_file, "w") as file:#Then it opens the file in write mode
            file.write(last_read_timestamp)#It writes the updated_time_stamp
    print("All Data successfully published to Kafka")#If all data is pushed into topic from source
    time.sleep(120)  # Wait for 120 seconds before checking for new data again
