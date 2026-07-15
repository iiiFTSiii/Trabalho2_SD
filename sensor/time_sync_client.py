import json
import threading
import time
import uuid

import pika

from comum.clock_sync import compute_cristian_offset

TIME_REQUEST_EXCHANGE = 'time_sync_request'
SYNC_INTERVAL = 10.0
RPC_TIMEOUT = 3.0


class TimeSyncClient:
    """Roda em background no sensor. Periodicamente pede a hora a um
    servidor de tempo (semaforo lider) usando o proprio middleware
    Pub-Sub e aplica o Algoritmo de Cristian para corrigir a deriva
    do relogio local."""

    def __init__(self, rabbitmq_host, node_id, drifting_clock):
        self.rabbitmq_host = rabbitmq_host
        self.node_id = node_id
        self.clock = drifting_clock
        self._stop = threading.Event()

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                self._sync_once()
            except Exception as exc:
                print(f"[{self.node_id}] Falha na sincronizacao de relogio: {exc}", flush=True)
            time.sleep(SYNC_INTERVAL)

    def _sync_once(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
        try:
            channel = connection.channel()
            channel.exchange_declare(exchange=TIME_REQUEST_EXCHANGE, exchange_type='fanout')

            result = channel.queue_declare(queue='', exclusive=True)
            reply_queue = result.method.queue

            correlation_id = str(uuid.uuid4())
            response = {}

            def on_response(ch, method, properties, body):
                if properties.correlation_id == correlation_id:
                    response['payload'] = json.loads(body)
                    response['t1'] = self.clock.now()

            channel.basic_consume(queue=reply_queue, on_message_callback=on_response, auto_ack=True)

            t0 = self.clock.now()
            channel.basic_publish(
                exchange=TIME_REQUEST_EXCHANGE,
                routing_key='',
                properties=pika.BasicProperties(reply_to=reply_queue, correlation_id=correlation_id),
                body=json.dumps({'node_id': self.node_id, 't0': t0}),
            )

            deadline = time.time() + RPC_TIMEOUT
            while 'payload' not in response and time.time() < deadline:
                connection.process_data_events(time_limit=0.2)

            if 'payload' not in response:
                print(f"[{self.node_id}] Sem resposta do servidor de tempo (timeout).", flush=True)
                return

            server_time = response['payload']['server_time']
            t1 = response['t1']
            offset, rtt = compute_cristian_offset(t0, t1, server_time)
            self.clock.apply_sync_offset(offset)
            print(
                f"[{self.node_id}] Sincronizado via Cristian: offset={offset:.4f}s rtt={rtt:.4f}s "
                f"correcao_acumulada={self.clock.current_correction:.4f}s",
                flush=True,
            )
        finally:
            try:
                connection.close()
            except Exception:
                pass
