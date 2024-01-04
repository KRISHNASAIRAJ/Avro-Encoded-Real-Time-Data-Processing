import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config = {
    "bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",  # The host url of kafka server(cloud)
    "sasl.mechanisms": "PLAIN",  # SASL mechanisms for authentication
    "security.protocol": "SASL_SSL",  # Security Protocol
    "sasl.username": "SQIODPYLDQYOGBI6",  # SASL USERNAME(API)
    "sasl.password": "PE7oqEnZJmfLlOv4GmsAROOKJv4yLbawFOu7ShoJgt1xkYP/dIOKgX9FB3gUls8R",  # SASL PASSWORD(API)
    "group.id": "group11",  # Groupid of the consumer
    "auto.offset.reset": "latest",  # Consuming latest data we can also use 'earliest'
}
# Create a Schema Registry client which manages the updated schema
schema_registry_client = SchemaRegistryClient(
    {
        "url": "https://psrc-5j7x8.us-central1.gcp.confluent.cloud",  # The host url of schema registry
        "basic.auth.user.info": "{}:{}".format(
            "2PTX5PP5K2ZK2KGL",  # Schema Registry API key
            "3g3RrACXhg2uMG91o411u5LsLHNeMH2A8SoAsH4pmYzRoATvAJo7ORQwRtee0J7+",  # Schema registry API value/secret
        ),
    }
)
# Fetch the latest Avro schema for the value
subject_name = "product_updates-value"  # topic_name+(-value)
schema_str = schema_registry_client.get_latest_version(
    subject_name
).schema.schema_str  # It gives the latest updated schema version as string(actual one in ui)
# Create Avro Deserializer for the value
key_deserializer = StringDeserializer(
    "utf_8"
)  # DeSerializer for keys, encoded as strings
avro_deserializer = AvroDeserializer(
    schema_registry_client, schema_str
)  # DeSerializer for Avro schema
# Define the DeserializingConsumer object
consumer = DeserializingConsumer(
    {
        "bootstrap.servers": kafka_config["bootstrap.servers"],  # Kafka host server url
        "security.protocol": kafka_config[
            "security.protocol"
        ],  # Security protocol configuration
        "sasl.mechanisms": kafka_config[
            "sasl.mechanisms"
        ],  # SASL mechanisms for authentication
        "sasl.username": kafka_config["sasl.username"],  # SASL USERNAME(API)
        "sasl.password": kafka_config["sasl.password"],  # SASL PASSWORD(API)
        "key.deserializer": key_deserializer,  # Key will be Deserialized as a string
        "value.deserializer": avro_deserializer,  # Value will be deserialized as Avro
        "group.id": kafka_config["group.id"],  # Groupid of the consumer
        "auto.offset.reset": kafka_config["auto.offset.reset"],  # Consuming data offset
        # 'enable.auto.commit': True, #Enabling auto commit
        # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds which is manual and sync()
    }
)
consumer.subscribe(["product_updates"])  # Consumer object is subscribing to kafka topic


# Function to apply transformation logic
def transform_data(data):
    data["category"] = data["category"].upper()
    if data["category"] == "Electronics":
        data["price"] *= 0.9  # Applying a 10% discount on electronics
    return data


# Function to write transformed data to JSON file
def write_to_json_file(data):
    with open("transformed_data.json", "a") as file:
        json.dump(data, file)
        file.write("\n")  # Writing each record in a new line


try:
    while True:
        message = consumer.poll(1.0)  # Waiting time for message (in seconds)
        if message is None:  # If message is not recieved
            continue
        if message.error():  # If error occured
            print("Consumer error: {}".format(message.error()))
            continue
        # Deserialize Avro data
        key = message.key()  # we have used key as product_id
        value = (
            message.value()
        )  # The total fields in the message(prod_id,name,category,timestamp)
        transformed_data = transform_data(value)  # Apply data transformation
        write_to_json_file(transformed_data)  # Writing to json file
        print("Successfully consumed record with key {}".format(message.key()))
except KeyboardInterrupt:  # To interupt the consumer process
    pass
finally:  # Finally close the consumer connection
    consumer.close()
