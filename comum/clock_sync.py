"""
Sincronizacao fisica de relogios - variacao do Algoritmo de Cristian.

Diferente da versao anterior (que so calculava um offset local sem
nenhuma troca de mensagens real), aqui o offset e calculado a partir
de uma requisicao/resposta feita atraves do proprio middleware Pub-Sub
(RabbitMQ), medindo o RTT (round-trip time) real da requisicao.

Fluxo:
  1. O sensor guarda t0 = seu relogio local no instante do envio.
  2. Publica uma mensagem de TIME_REQUEST e aguarda a resposta.
  3. Um servidor de tempo (o semaforo lider, que nao sofre deriva
     artificial) responde com seu horario `server_time`.
  4. O sensor guarda t1 = seu relogio local no instante do recebimento.
  5. Aplica o Algoritmo de Cristian classico:
        RTT = t1 - t0
        tempo_ajustado_do_servidor = server_time + RTT / 2
        offset = tempo_ajustado_do_servidor - t1
  6. O sensor passa a somar `offset` ao proprio relogio fisico em toda
     mensagem publicada dali em diante, ate a proxima resincronizacao.
"""

import time


class CristianOffsetSample:
    __slots__ = ("offset", "rtt", "timestamp")

    def __init__(self, offset, rtt, timestamp=None):
        self.offset = offset
        self.rtt = rtt
        self.timestamp = timestamp if timestamp is not None else time.time()


def compute_cristian_offset(t0, t1, server_time):
    """Calcula o offset de relogio pelo Algoritmo de Cristian.

    t0: horario local (do cliente) no momento do envio da requisicao.
    t1: horario local (do cliente) no momento do recebimento da resposta.
    server_time: horario informado pelo servidor de tempo na resposta.

    Retorna (offset, rtt). O offset deve ser SOMADO ao relogio local
    do cliente para aproxima-lo do relogio do servidor.
    """
    rtt = t1 - t0
    if rtt < 0:
        rtt = 0.0
    adjusted_server_time = server_time + (rtt / 2.0)
    offset = adjusted_server_time - t1
    return offset, rtt


class DriftingClock:
    """Relogio fisico local com deriva artificial simulada e correcao
    aplicada por sincronizacao (Cristian) ao longo do tempo."""

    def __init__(self, drift_offset_seconds=0.0):
        self._drift_offset = drift_offset_seconds
        self._sync_offset = 0.0

    def now(self):
        return time.time() + self._drift_offset + self._sync_offset

    def apply_sync_offset(self, offset):
        self._sync_offset += offset

    @property
    def current_correction(self):
        return self._sync_offset
