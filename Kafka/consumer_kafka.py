# Kafka Consumer
from datetime import datetime
from kafka import KafkaConsumer
import pyodbc

topic='toll'

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                    'Server=MSI\SQLEXPRESS;'
                    'Database=toll_data;'
                    'Trusted_Connection=yes;')

cursor = conn.cursor()

# initializing consumer to consume the messages sent by producer
consumer = KafkaConsumer(TOPIC, bootstrap_servers='localhost:9092', auto_offset_reset = 'earliest', group_id=None,)

for msg in consumer:

    # Decoding the received messages because consumer only accepts data in bytes so we have to decode
    message = msg.value.decode("utf-8")

    # Transforming the timestamp
    (timestamp, vehicle_id, vehicle_type, plaza_id) = message.split(",")
    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table
    loading_data = cursor.execute("insert into tolldata values(%s,%s,%s,%s)", (timestamp, vehicle_id, vehicle_type, plaza_id))
    connection.commit()
connection.close()
