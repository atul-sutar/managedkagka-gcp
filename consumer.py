import confluent_kafka



# This example uses Confluent's Python Client for Apache Kafka
# https://github.com/confluentinc/confluent-kafka-python
# Update to your clusters specific metadata
kafka_cluster_name = ''
region = ''
project_id = ''
port = ''
kafka_topic_name = "my-topic"
# bootstrap_hostname = f'bootstrap.{kafka_cluster_name}.{region}.managedkafka.{project_id}.cloud.goog:{port}'
bootstrap_hostname = "bootstrap.my-cluster.us-central1.managedkafka.genai-vertexai-llm-journey.cloud.goog:9092"

# Set up the config for Kafka
# Using OAUTHBEARER mechanism and local auth server
conf = {
  'bootstrap.servers': bootstrap_hostname,
  'group.id': 'group1',
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'OAUTHBEARER',
  'sasl.oauthbearer.token.endpoint.url': 'localhost:8080',
  'sasl.oauthbearer.client.id': 'unused',
  'sasl.oauthbearer.client.secret': 'unused',
  'sasl.oauthbearer.method': 'oidc'
}

consumer = confluent_kafka.Consumer(conf)
consumer.subscribe([kafka_topic_name])

try:
    while (True):
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            print("Something went wrong...")

        print(msg.value())
except KeyboardInterrupt:
    print('Canceled by user.')
finally:
    consumer.close()
