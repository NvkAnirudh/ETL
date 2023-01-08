# Kafka Producer
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer

# initializing kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# initializing toll name
topic = 'toll'

# Vehicle types passing through the toll plaza
vehicle_types = ("car", "truck", "van", "sports_car", "mini_van")

for _ in range(10):
    # Randomly generated vehicle id between 1 and 10
    vehicle_id = randint(1, 10)

    # Randomly chosen vehicle type from vehicle types declared above
    vehicle_type = choice(vehicle_types)

    # Randomly generated vehicle id between 10 and 20
    plaza_id = randint(10, 20)

    # Current time
    now = ctime(time())

    message = "{}".format(now) +','+ "{}".format(vehicle_id) +','+ "{}".format(vehicle_type) +','+ "{}".format(plaza_id)
    message = bytearray(message.encode("utf-8"))
    
    producer.send(TOPIC, message)


