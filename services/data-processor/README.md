```markdown
# Data Processor

Consumes messages from Kafka topic and writes cleaned/transformed output to MongoDB.

## Libraries Used
- `kafka-python`
- `pymongo`

## Environment Variables
- `KAFKA_BROKER`
- `KAFKA_TOPIC`
- `MONGO_URI`