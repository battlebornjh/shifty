import pika
import time
import json
import os
import Series
import logging
import sys
from datetime import date
from GetData import getSeriesData

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define RabbitMQ connection parameters (using environment variables for Docker compatibility)
AMQP_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
QUEUE_NAME = 'Shifty'

def callback(ch, method, properties, body):
    #"""Function to process received messages."""
    #print(f" [x] Received {body.decode()}")
    # Simulate some work
    #time.sleep(body.count(b'.'))
    #print(" [x] Done")
    # Acknowledge the message has been processed
    data = json.loads(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)
    sd = getSeriesData(date.fromisoformat(data["strt"]), date.fromisoformat(data["end"]), data["symbols"])
    Series.search_series(data["guid"], data["symbols"], sd.dates, sd.data, data["corNumber"], data["minShift"], data["size"])
    

def main():
    try:
        # Establish connection
        # We use a loop/retry logic here for resilience, as the consumer might start before RabbitMQ is ready
        credentials = pika.PlainCredentials("myuser", "mypassword")
        connection_params = pika.ConnectionParameters(host=AMQP_HOST, port=5672, heartbeat=360, credentials=credentials)
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Declare the queue (idempotent - creates if it doesn't exist)
        # durable=True ensures the queue survives a RabbitMQ restart
        channel.queue_declare(queue=QUEUE_NAME, durable=False)

        # Set prefetch count to 1 to ensure a worker only gets one message at a time
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=callback
        )

        print(f' [*] Waiting for messages on {QUEUE_NAME}. To exit press CTRL+C')
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ: {e}")
        time.sleep(5)
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    main()
