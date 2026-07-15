import json
import threading
import time

import pika

TIME_REQUEST_EXCHANGE = 'time_sync_request'


class TimeServer:
    """So responde a requisicoes de sincronizacao de tempo enquanto
    este no for o lider (evita multiplos 'servidores de tempo'
    divergentes respondendo ao mesmo tempo). Os semaforos nao sofrem
    deriva artificial, entao servem como referencia de tempo fisico
    confiavel para o Algoritmo de Cristian, sem depender de NTP
    externo (proibido pelo enunciado)."""

    def __init__(self, rabbitmq_host, node_id, is_leader_fn):
        self.rabbitmq_host = rabbitmq_host
        self.node_id = node_id
        self.is_leader_fn = is_leader_fn
        self._stop = threading.Event()

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                self._consume()
            except Exception as exc:
                print(f"[{self.node_id}] TimeServer: reconectando apos erro: {exc}", flush=True)
                time.sleep(3)

    def _consume(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
        channel = connection.channel()
        channel.exchange_declare(exchange=TIME_REQUEST_EXCHANGE, exchange_type='fanout')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=TIME_REQUEST_EXCHANGE, queue=queue_name)

        def on_request(ch, method, properties, body):
            if not self.is_leader_fn():
                return
            request = json.loads(body)
            reply = {'server_time': time.time(), 'served_by': self.node_id}
            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                body=json.dumps(reply),
            )
            print(f"[{self.node_id}] Respondeu sync de tempo para {request.get('node_id')}", flush=True)

        channel.basic_consume(queue=queue_name, on_message_callback=on_request, auto_ack=True)
        channel.start_consuming()
