import json
import os
import random
import time

import pika

from comum.clock_sync import DriftingClock
from sensor.logical_clock import LamportClock
from sensor.time_sync_client import TimeSyncClient

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

    # Deriva artificial de relogio fisico (Restricao C). Cada sensor
    # acelera ou atrasa seu proprio relogio em relacao ao host.
    drift_offset = random.uniform(-8.0, 8.0)
    physical_clock = DriftingClock(drift_offset_seconds=drift_offset)
    print(f"[{NODE_ID}] Deriva de tempo artificial injetada: {drift_offset:.2f}s", flush=True)

    # Cliente de sincronizacao Cristian: pede a hora ao semaforo lider
    # periodicamente via o proprio middleware Pub-Sub e corrige a
    # deriva. E proibido consultar NTP externo.
    time_sync = TimeSyncClient(RABBITMQ_HOST, NODE_ID, physical_clock)
    time_sync.start()

    while True:
        try:
            clock.tick()
            cars_count = random.randint(0, 20)

            message = {
                'sensor_id': NODE_ID,
                'cars': cars_count,
                'lamport_time': clock.get_time(),
                'physical_time_raw': time.time() + drift_offset,
                'physical_time_synced': physical_clock.now(),
                'clock_correction_applied': physical_clock.current_correction,
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
            channel.exchange_declare(exchange='traffic_data', exchange_type='fanout')


if __name__ == '__main__':
    start_publishing()
