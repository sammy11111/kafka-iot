import requests
import time
import json
import os
from kafka import KafkaProducer
from multiprocessing import Pool
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from typing import List, Optional

# Number of workers in the pool
# This is the number of concurrent requests that can be made to the API
POOL_SIZE = 8
TIME_WINDOW = 60  # seconds
SENSOR_DATA_TOPIC = 'iot.raw-data.opensensemap'
KAFKA_BOOTSTRAP_SERVER = os.environ['KAFKA_BOOTSTRAP_SERVER'] or 'localhost:29092'

# Models for API calls to fetch boxes
class Location(BaseModel):
    coordinates: list[float]
    type: str
    timestamp: datetime

class Box(BaseModel):
    _id:str
    name: str
    exposure: Optional[str]
    lastMeasurementAt: datetime
    currentLocation: Location

# Models for API calls to fetch sensors
class Measurement(BaseModel):
    createdAt: datetime
    value: float

class Sensor(BaseModel):
    _id: str
    title: str
    unit: str
    sensorType: str
    lastMeasurement: Measurement

# Get all sensor boxes from the API and return the ones that have been updated since "updatedSince"
# returns a list of boxes with their id, name, latitude, longitude, height and exposure


def getUpdatedBox(updatedSince):
    print("Getting updated boxes since: " + str(updatedSince) + '...')
    # Get all sensor boxes from the API
    boxes = requests.get('https://api.opensensemap.org/boxes')
    # Filter the boxes that have been updated since "updatedSince"
    rtn = []
    for box in boxes.json():
        #print(box)
        try:
            box_data = Box.model_validate(box)
            #print(box_data)
            if int(time.mktime(time.strptime(box['lastMeasurementAt'], '%Y-%m-%dT%H:%M:%S.%fZ'))) > int(updatedSince):
                box_dict = {
                    'id': box['_id'],
                    'name': box['name'],
                    'exposure': box.get('exposure', None),
                }
                if box['currentLocation'] and box['currentLocation']['coordinates']:
                    box_dict['lat'] = box['currentLocation']['coordinates'][0]
                    box_dict['lon'] = box['currentLocation']['coordinates'][1]
                    if len(box['currentLocation']['coordinates']) == 3:
                        box_dict['height'] = box['currentLocation']['coordinates'][2]
                rtn.append(box_dict)
        except ValidationError as e:
            next
    print("Boxes updated since " + str(updatedSince) + ": " + str(len(rtn)))
    # Return the boxes that have been updated since "updatedSince"
    return rtn

# Get the latest measurement from sensor boxes and send it to Kafka


def getLastMeasurement(box):
    measurements = requests.get(
        'https://api.opensensemap.org/boxes/' + box['id'] + '/sensors')
    try:
        # Configure the Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        if not producer.bootstrap_connected():
            print("Producer is not connected to Kafka")
            exit(1)
        for sensor in measurements.json()['sensors']:
            #print(sensor)
            try:
                sensor_data = Sensor.model_validate(sensor)
                #print(sensor_data)
                message = {
                    'boxId': box['id'],
                    'boxName': box['name'],
                    'exposure': box['exposure'],
                    'lat': box['lat'],
                    'lon': box['lon'],
                    'height': box.get('height', None),
                    'sensorId': sensor['_id'],
                    'sensorType': sensor.get('sensorType', None),
                    'phenomenon': sensor.get('title', None),
                    'value': sensor['lastMeasurement']['value'],
                    'unit': sensor.get('unit', None),
                    'createdAt': sensor['lastMeasurement']['createdAt']
                }
                #print(message)
                try:
                    producer.send(SENSOR_DATA_TOPIC, message)
                    print("Sensor data sent to Kafka")
                except Exception as e:
                    print("Error sending message to Kafka: " + str(e))
            except ValidationError as e:
                print("Invlaid sensor data: "+ str(e))
                next
        producer.flush()
    except Exception as e:
        print("Cannot connect to Kafka: " + str(e))
        exit(1)


if __name__ == '__main__':
    # continuously get the latest measurements from the sensor boxes
    while True:
        # look for boxes with measurements updated in the last window
        boxes = getUpdatedBox(time.time() - TIME_WINDOW)
        p = Pool(POOL_SIZE)
        # get the latest measurement for all boxes
        p.map(getLastMeasurement, boxes)
        p.close()
        p.join()
