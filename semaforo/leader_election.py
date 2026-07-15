import json
import os
import threading
import time

import pika

NODE_ID = os.getenv('NODE_ID', 'node_unknown')
TOTAL_NODES = int(os.getenv('TOTAL_NODES', 1))
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0
MONITOR_INTERVAL = 4.0


def evaluate_quorum(active_count, total_nodes):
    """Maioria estrita (> 50% do total de nos), nao apenas >= metade.
    Com total par (ex.: 4 nos divididos 2x2), nenhum dos dois lados
    isolados atinge quorum -> nenhum lado elege lider (anti split-brain)."""
    quorum_required = (total_nodes // 2) + 1
    return active_count >= quorum_required


def find_inactive_nodes(heartbeats, now=None, timeout=HEARTBEAT_TIMEOUT):
    if now is None:
        now = time.time()
    return [node_id for node_id, last_seen in heartbeats.items() if (now - last_seen) > timeout]


def connect_with_retry(rabbitmq_host, retry_delay=3.0, label=""):
    """Abre uma conexao bloqueante com retry infinito. O healthcheck do
    docker-compose (depends_on: condition: service_healthy) reduz a
    chance de corrida, mas nao a elimina (o RabbitMQ pode responder ao
    diagnostico e ainda nao aceitar novas conexoes de cliente por um
    instante). Sem isso, qualquer soluco de inicializacao derruba o
    container de vez, ja que os semaforos rodam com restart: "no"."""
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        except pika.exceptions.AMQPConnectionError:
            print(f"[{NODE_ID}]{(' ' + label) if label else ''} RabbitMQ indisponivel, tentando novamente em {retry_delay}s...", flush=True)
            time.sleep(retry_delay)


class HeartbeatMonitor:
    """Publica heartbeats periodicos e escuta os heartbeats dos demais
    nos via um exchange fanout dedicado. Mantem o timestamp do ultimo
    heartbeat visto de cada no, usado para detectar falhas e para
    calcular quorum durante a eleicao."""

    def __init__(self, rabbitmq_host):
        self.rabbitmq_host = rabbitmq_host
        self.connection = connect_with_retry(rabbitmq_host, label="HeartbeatMonitor")
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='heartbeat_exchange', exchange_type='fanout')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange='heartbeat_exchange', queue=self.queue_name)

        self._lock = threading.Lock()
        self.heartbeats = {NODE_ID: time.time()}
        self._stop = threading.Event()

    def _consume(self):
        def _callback(ch, method, properties, body):
            payload = json.loads(body.decode('utf-8'))
            node_id = payload.get('node_id', 'unknown')
            with self._lock:
                self.heartbeats[node_id] = payload.get('timestamp', time.time())

        while not self._stop.is_set():
            try:
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=_callback, auto_ack=True)
                self.channel.start_consuming()
            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosed, pika.exceptions.AMQPChannelError):
                print(f"[{NODE_ID}] HeartbeatMonitor(consume) perdeu conexao, reconectando...", flush=True)
                self.connection = connect_with_retry(self.rabbitmq_host, label="HeartbeatMonitor(consume)")
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange='heartbeat_exchange', exchange_type='fanout')
                result = self.channel.queue_declare(queue='', exclusive=True)
                self.queue_name = result.method.queue
                self.channel.queue_bind(exchange='heartbeat_exchange', queue=self.queue_name)

    def start(self):
        threading.Thread(target=self._consume, daemon=True).start()
        threading.Thread(target=self._publish_loop, daemon=True).start()

    def _publish_loop(self):
        # Conexao propria para publicar, para nao disputar o canal
        # usado pelo consumo (BlockingConnection nao e thread-safe).
        connection = connect_with_retry(self.rabbitmq_host, label="HeartbeatMonitor(publish)")
        channel = connection.channel()
        channel.exchange_declare(exchange='heartbeat_exchange', exchange_type='fanout')
        while not self._stop.is_set():
            now = time.time()
            with self._lock:
                self.heartbeats[NODE_ID] = now
            payload = json.dumps({'node_id': NODE_ID, 'timestamp': now})
            try:
                channel.basic_publish(exchange='heartbeat_exchange', routing_key='', body=payload)
            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosed, pika.exceptions.AMQPChannelError):
                print(f"[{NODE_ID}] HeartbeatMonitor(publish) perdeu conexao, reconectando...", flush=True)
                connection = connect_with_retry(self.rabbitmq_host, label="HeartbeatMonitor(publish)")
                channel = connection.channel()
                channel.exchange_declare(exchange='heartbeat_exchange', exchange_type='fanout')
            time.sleep(HEARTBEAT_INTERVAL)

    def stop(self):
        self._stop.set()

    def get_heartbeats(self):
        with self._lock:
            return dict(self.heartbeats)

    def get_active_nodes(self, timeout=HEARTBEAT_TIMEOUT, now=None):
        if now is None:
            now = time.time()
        with self._lock:
            return {n: ts for n, ts in self.heartbeats.items() if (now - ts) <= timeout}

    def get_inactive_nodes(self, now=None):
        return find_inactive_nodes(self.get_heartbeats(), now=now, timeout=HEARTBEAT_TIMEOUT)


class BullyElection:
    """Variante simplificada do algoritmo do Bully: em vez de trocar
    mensagens ELECTION/OK/COORDINATOR par a par, usa a visao de
    heartbeats (quem esta ativo agora) para decidir, de forma
    deterministica, qual e o maior NODE_ID ativo. So declara um lider
    se houver quorum de maioria estrita; caso contrario entra em modo
    seguro (sem lider), prevenindo split-brain durante particoes de
    rede. Reage automaticamente a falhas: um monitor em background
    detecta quando o lider atual para de mandar heartbeat e dispara
    uma nova eleicao (docker kill do lider, particao, etc.)."""

    def __init__(self, rabbitmq_host, heartbeat_monitor):
        self.rabbitmq_host = rabbitmq_host
        self.connection = connect_with_retry(rabbitmq_host, label="BullyElection")
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='election_exchange', exchange_type='fanout')

        self.heartbeat_monitor = heartbeat_monitor
        self._lock = threading.Lock()
        self.is_leader = False
        self.leader_id = None
        self._stop = threading.Event()

    # --- API publica ---------------------------------------------------
    def is_leader_now(self):
        with self._lock:
            return self.is_leader

    def current_leader(self):
        with self._lock:
            return self.leader_id

    def start_background_monitor(self):
        threading.Thread(target=self._listen_announcements, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    def stop(self):
        self._stop.set()

    def _safe_publish(self, body):
        try:
            self.channel.basic_publish(exchange='election_exchange', routing_key='', body=body)
        except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosed, pika.exceptions.AMQPChannelError):
            print(f"[{NODE_ID}] Conexao de eleicao perdida, reconectando...", flush=True)
            try:
                self.connection = connect_with_retry(self.rabbitmq_host, label="BullyElection(reconnect)")
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange='election_exchange', exchange_type='fanout')
                self.channel.basic_publish(exchange='election_exchange', routing_key='', body=body)
            except Exception as exc:
                print(f"[{NODE_ID}] Falha ao republicar apos reconexao: {exc}", flush=True)

    # --- eleicao ---------------------------------------------------------
    def start_election(self):
        print(f"[{NODE_ID}] Iniciando eleicao...", flush=True)
        active_nodes = self.heartbeat_monitor.get_active_nodes()
        active_count = len(active_nodes)

        self._safe_publish(f"ELECTION:{NODE_ID}")

        if not evaluate_quorum(active_count, TOTAL_NODES):
            print(
                f"[{NODE_ID}] QUORUM NAO ATINGIDO ({active_count}/{TOTAL_NODES}). "
                f"Entrando em SAFE MODE (sem lider).",
                flush=True,
            )
            with self._lock:
                self.is_leader = False
                self.leader_id = None
            return False

        maior_id = max(active_nodes.keys())
        with self._lock:
            self.leader_id = maior_id
            self.is_leader = (maior_id == NODE_ID)

        if self.is_leader:
            print(f"[{NODE_ID}] Quorum OK ({active_count}/{TOTAL_NODES}). Eu sou o novo lider.", flush=True)
        else:
            print(
                f"[{NODE_ID}] Quorum OK ({active_count}/{TOTAL_NODES}). Novo lider e {maior_id}.",
                flush=True,
            )

        self._safe_publish(f"LEADER:{maior_id}")
        return self.is_leader

    # --- background --------------------------------------------------------
    def _listen_announcements(self):
        def _callback(ch, method, properties, body):
            text = body.decode('utf-8')
            if text.startswith('LEADER:'):
                announced = text.split(':', 1)[1]
                print(f"[{NODE_ID}] Anuncio de lideranca recebido: {announced}", flush=True)

        while not self._stop.is_set():
            try:
                connection = connect_with_retry(self.rabbitmq_host, label="BullyElection(listener)")
                channel = connection.channel()
                channel.exchange_declare(exchange='election_exchange', exchange_type='fanout')
                result = channel.queue_declare(queue='', exclusive=True)
                queue_name = result.method.queue
                channel.queue_bind(exchange='election_exchange', queue=queue_name)
                channel.basic_consume(queue=queue_name, on_message_callback=_callback, auto_ack=True)
                channel.start_consuming()
            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosed, pika.exceptions.AMQPChannelError):
                print(f"[{NODE_ID}] BullyElection(listener) perdeu conexao, reconectando...", flush=True)

    def _monitor_loop(self):
        # Reeleicao automatica: dispara nova eleicao quando o lider
        # conhecido some dos heartbeats ativos (docker kill, particao,
        # etc.) ou quando ainda nao ha lider definido.
        while not self._stop.is_set():
            time.sleep(MONITOR_INTERVAL)
            active_nodes = self.heartbeat_monitor.get_active_nodes()
            with self._lock:
                leader = self.leader_id
            if leader is None or leader not in active_nodes:
                print(f"[{NODE_ID}] Lider '{leader}' ausente/inativo. Disparando nova eleicao.", flush=True)
                self.start_election()