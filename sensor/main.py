import json
import os
import random
import time

import pika

from comum.clock_sync import sync_clock_cristian
from sensor.logical_clock import LamportClock

NODE_ID = os.getenv('NODE_ID', 'sensor_unknown')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

clock = LamportClock()


def connect_broker():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(f"[{NODE_ID}] RabbitMQ indisponível. Tentando novamente em 3s...", flush=True)
            time.sleep(3)


def start_publishing():
    connection = connect_broker()
    channel = connection.channel()
    channel.exchange_declare(exchange='traffic_data', exchange_type='fanout')

    physical_time_offset = random.uniform(-5.0, 5.0)
    print(f"[{NODE_ID}] Deriva de tempo inicial: {physical_time_offset:.2f}s", flush=True)

    while True:
        try:
            clock.tick()
            cars_count = random.randint(0, 20)
            current_physical_time = time.time() + physical_time_offset

            message = {
                'sensor_id': NODE_ID,
                'cars': cars_count,
                'lamport_time': clock.get_time(),
                'physical_time': current_physical_time,
                'clock_offset': sync_clock_cristian(current_physical_time, current_physical_time, 0),
            }

            channel.basic_publish(
                exchange='traffic_data',
                routing_key='',
                body=json.dumps(message)
            )
            print(f"[{NODE_ID}] Publicado: {message}", flush=True)
            time.sleep(2)

        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError, pika.exceptions.AMQPConnectionError):
            print(f"[{NODE_ID}] Conexão perdida. Reconectando...", flush=True)
            connection = connect_broker()
            channel = connection.channel()


if __name__ == '__main__':
    start_publishing()