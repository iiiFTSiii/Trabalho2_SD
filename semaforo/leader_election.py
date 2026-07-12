import json
import os
import threading
import time

import pika

NODE_ID = os.getenv('NODE_ID', 'node_unknown')
TOTAL_NODES = int(os.getenv('TOTAL_NODES', 1))
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 5.0


def evaluate_quorum(active_count, total_nodes):
    quorum_required = (total_nodes // 2) + 1
    return active_count >= quorum_required


def find_inactive_nodes(heartbeats, now=None, timeout=HEARTBEAT_TIMEOUT):
    if now is None:
        now = time.time()
    return [node_id for node_id, last_seen in heartbeats.items() if (now - last_seen) > timeout]


class HeartbeatMonitor:
    def __init__(self, connection):
        self.connection = connection
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='heartbeat_exchange', exchange_type='fanout')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange='heartbeat_exchange', queue=self.queue_name)

        self.heartbeats = {NODE_ID: time.time()}
        self._stop = threading.Event()
        self._thread = None

    def _consume(self):
        def _callback(ch, method, properties, body):
            payload = json.loads(body.decode('utf-8'))
            node_id = payload.get('node_id', 'unknown')
            self.heartbeats[node_id] = payload.get('timestamp', time.time())

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=_callback, auto_ack=True)
        self.channel.start_consuming()

    def start(self):
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()
        while not self._stop.is_set():
            self.publish_heartbeat()
            time.sleep(HEARTBEAT_INTERVAL)

    def stop(self):
        self._stop.set()
        try:
            self.connection.close()
        except Exception:
            pass

    def publish_heartbeat(self):
        payload = json.dumps({'node_id': NODE_ID, 'timestamp': time.time()})
        self.channel.basic_publish(exchange='heartbeat_exchange', routing_key='', body=payload)

    def get_heartbeats(self):
        return dict(self.heartbeats)

    def get_inactive_nodes(self, now=None):
        return find_inactive_nodes(self.heartbeats, now=now, timeout=HEARTBEAT_TIMEOUT)


class BullyElection:
    def __init__(self, connection, heartbeat_monitor=None):
        self.connection = connection
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='election_exchange', exchange_type='fanout')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange='election_exchange', queue=self.queue_name)

        self.is_leader = False
        self.leader_id = None
        self.active_nodes = {NODE_ID: time.time()}
        self.heartbeat_monitor = heartbeat_monitor

    def start_election(self):
        print(f"[{NODE_ID}] Iniciando eleição...", flush=True)
        if self.heartbeat_monitor is not None:
            self.active_nodes = {NODE_ID: time.time(), **self.heartbeat_monitor.get_heartbeats()}
        else:
            self.active_nodes = {NODE_ID: time.time()}

        self.channel.basic_publish(
            exchange='election_exchange', routing_key='', body=f"ELECTION:{NODE_ID}"
        )
        time.sleep(2)

        active_count = len(self.active_nodes)
        if not evaluate_quorum(active_count, TOTAL_NODES):
            print(f"[{NODE_ID}] QUÓRUM NÃO ATINGIDO ({active_count}/{TOTAL_NODES}). Entrando em SAFE MODE.", flush=True)
            self.is_leader = False
            return False

        maior_id = max(self.active_nodes.keys())
        if maior_id == NODE_ID:
            print(f"[{NODE_ID}] Eu sou o novo líder.", flush=True)
            self.is_leader = True
            self.leader_id = NODE_ID
            self.channel.basic_publish(
                exchange='election_exchange', routing_key='', body=f"LEADER:{NODE_ID}"
            )
        else:
            self.is_leader = False
            print(f"[{NODE_ID}] Perdi a eleição. Maior ID é {maior_id}", flush=True)
        return self.is_leader