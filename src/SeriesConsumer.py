from GetData import getSeriesData
import pika
import json
from datetime import date
import Series
import os

def consume():
    RABBIT_HOST = os.environ.get("RABBITMQ_HOST", "localhost")

    credentials = pika.PlainCredentials("myuser", "mypassword")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials))

    channel = connection.channel()

    channel.queue_declare(queue='Shifty')

    while True:
        method_frame, header_frame, body = channel.basic_get(queue='Shifty', auto_ack=True)

        if method_frame:
            data = json.loads(body.decode())
            print(f"got {data['size']}")
            sd = getSeriesData(date.fromisoformat(data["strt"]), date.fromisoformat(data["end"]), data["symbols"])
            Series.search_series(data["guid"], data["symbols"], sd.dates, sd.data, data["corNumber"], data["minShift"], data["size"])
        else:
            break

    connection.close()

consume()