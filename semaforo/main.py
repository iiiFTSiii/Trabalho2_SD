import json
import os
import threading
import time

import pika

from comum.fault_tolerance import load_checkpoint, save_checkpoint
from semaforo.leader_election import BullyElection, HeartbeatMonitor
from semaforo.time_server import TimeServer

NODE_ID = os.getenv('NODE_ID', 'semaforo_unknown')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

# Janela de reordenacao causal: mensagens sao acumuladas por esse
# tempo antes de serem processadas em ordem (lamport_time, sensor_id).
# Isso resolve dois problemas da versao anterior:
#   1) Empates de lamport_time entre sensores diferentes agora tem
#      desempate deterministico (sensor_id), garantindo ordem total.
#   2) Perda de pacotes (5% conforme Restricao A) nao trava mais o
#      processamento para sempre: antes o codigo esperava um numero de
#      sequencia exato que, se perdido, travava o buffer para sempre.
#      Agora o buffer e liberado por tempo, nao por sequencia exata.
REORDER_WINDOW_SECONDS = 5.0

message_buffer = []
buffer_lock = threading.Lock()
processed_count = 0


def process_buffer_periodically():
    global processed_count
    while True:
        time.sleep(REORDER_WINDOW_SECONDS)
        with buffer_lock:
            if not message_buffer:
                continue
            pending = sorted(message_buffer, key=lambda m: (m['lamport_time'], m['sensor_id']))
            message_buffer.clear()

        for msg in pending:
            processed_count += 1
            print(f"[{NODE_ID}] Processando causalmente (#{processed_count}): {msg}", flush=True)

        last_msg = pending[-1]
        save_checkpoint({
            'processed_count': processed_count,
            'last_lamport': last_msg['lamport_time'],
            'last_msg': last_msg,
        })


def callback(ch, method, properties, body):
    msg = json.loads(body)
    print(f"[{NODE_ID}] Recebido da rede: sensor={msg['sensor_id']} lamport={msg['lamport_time']}", flush=True)
    with buffer_lock:
        message_buffer.append(msg)


def start_subscriber():
    global processed_count
    estado_anterior = load_checkpoint()
    if estado_anterior:
        processed_count = estado_anterior.get('processed_count', 0)
        print(f"[{NODE_ID}] Recuperacao concluida. Mensagens ja processadas antes da queda: {processed_count}", flush=True)

    # Heartbeat + eleicao rodam em conexoes proprias e continuam
    # ativos durante toda a vida do processo, reagindo a falhas.
    heartbeat_monitor = HeartbeatMonitor(RABBITMQ_HOST)
    heartbeat_monitor.start()

    election = BullyElection(RABBITMQ_HOST, heartbeat_monitor)
    election.start_background_monitor()
    # Da um tempo para os heartbeats dos outros nos chegarem antes da
    # primeira eleicao, senao o proprio no se ve sozinho.
    threading.Timer(3.0, election.start_election).start()

    time_server = TimeServer(RABBITMQ_HOST, NODE_ID, election.is_leader_now)
    time_server.start()

    threading.Thread(target=process_buffer_periodically, daemon=True).start()

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
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
