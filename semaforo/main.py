import json
import os
import threading
import time

import pika

from comum.fault_tolerance import load_checkpoint, save_checkpoint
from semaforo.leader_election import BullyElection

NODE_ID = os.getenv('NODE_ID', 'semaforo_unknown')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

message_buffer = []
EXPECTED_LAMPORT = 1


def process_buffer():
    global EXPECTED_LAMPORT
    message_buffer.sort(key=lambda x: x['lamport_time'])

    while message_buffer and message_buffer[0]['lamport_time'] <= EXPECTED_LAMPORT:
        msg = message_buffer.pop(0)
        print(f"[{NODE_ID}] Processando causalmente: {msg}", flush=True)
        EXPECTED_LAMPORT = max(EXPECTED_LAMPORT, msg['lamport_time'] + 1)
        save_checkpoint({'expected_lamport': EXPECTED_LAMPORT, 'last_msg': msg})


def callback(ch, method, properties, body):
    msg = json.loads(body)
    print(f"[{NODE_ID}] Recebido da rede: {msg['lamport_time']}", flush=True)
    message_buffer.append(msg)
    process_buffer()


def start_subscriber():
    global EXPECTED_LAMPORT
    estado_anterior = load_checkpoint()
    if estado_anterior:
        EXPECTED_LAMPORT = estado_anterior.get('expected_lamport', 1)
        print(f"[{NODE_ID}] Recuperação concluída. Lamport: {EXPECTED_LAMPORT}", flush=True)

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            election_connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            election = BullyElection(election_connection)
            threading.Thread(target=election.start_election, daemon=True).start()

            channel.exchange_declare(exchange='traffic_data', exchange_type='fanout')
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange='traffic_data', queue=queue_name)

            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            print(f"[{NODE_ID}] Aguardando dados...", flush=True)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print(f"[{NODE_ID}] Broker caiu. Reconectando...", flush=True)
            time.sleep(3)


if __name__ == '__main__':
    start_subscriber()