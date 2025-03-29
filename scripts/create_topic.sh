#/bin/bash

echo -e 'Creating kafka topics...'
kafka-topics --create --if-not-exists --topic iot.raw-data.opensensemap --bootstrap-server kafka:29092
