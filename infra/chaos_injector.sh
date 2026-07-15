#!/bin/bash
set -e

NETWORK="sd_trafego_network"

usage() {
  echo "Uso: $0 {latency|partition|restore|kill-leader|status}"
  exit 1
}

if [ "$1" = "latency" ]; then
  echo "Aplicando latencia flutuante e perda de pacotes (Restricao A)..."
  # Latencia variavel entre ~10ms e ~4000ms, com 5% de perda, conforme
  # o enunciado. Usa distribuicao normal para variar a cada pacote.
  docker exec --privileged sensor_1 tc qdisc replace dev eth0 root netem delay 2000ms 1990ms distribution normal loss 5% || true
  docker exec --privileged sensor_2 tc qdisc replace dev eth0 root netem delay 500ms 490ms distribution normal loss 5% || true
  echo "OK. Verifique nos logos dos semaforos que as mensagens estao sendo reordenadas por lamport_time."
  exit 0
fi

if [ "$1" = "partition" ]; then
  echo "Simulando particao de rede 2x2 entre os semaforos (Restricao B)..."
  # semaforo_1 e semaforo_2 ficam de um lado; semaforo_3 e semaforo_4
  # do outro lado (desconectados da rede). Com TOTAL_NODES=4 e quorum
  # de maioria estrita = 3, NENHUM dos dois lados (2 cada) atinge
  # quorum -> nenhum lado elege lider, evitando split-brain.
  docker network disconnect "$NETWORK" semaforo_3 || true
  docker network disconnect "$NETWORK" semaforo_4 || true
  echo "semaforo_3 e semaforo_4 isolados. semaforo_1 e semaforo_2 permanecem na rede."
  echo "Esperado nos logs: os 4 nos entram em SAFE MODE (nenhum atinge 3/4)."
  exit 0
fi

if [ "$1" = "restore" ]; then
  echo "Restaurando rede..."
  docker exec --privileged sensor_1 tc qdisc del dev eth0 root netem || true
  docker exec --privileged sensor_2 tc qdisc del dev eth0 root netem || true
  docker network connect "$NETWORK" semaforo_3 || true
  docker network connect "$NETWORK" semaforo_4 || true
  echo "Rede restaurada. Uma nova eleicao deve ocorrer automaticamente (quorum 4/4)."
  exit 0
fi

if [ "$1" = "kill-leader" ]; then
  echo "Descobrindo o lider atual pelos logs..."
  LEADER=$(docker compose logs --no-color 2>/dev/null | grep -o "semaforo_[0-9] Eu sou o novo lider" | tail -1 | cut -d' ' -f1)
  if [ -z "$LEADER" ]; then
    echo "Nao foi possivel identificar o lider automaticamente nos logs."
    echo "Rode 'docker compose logs -f semaforo_1 semaforo_2 semaforo_3 semaforo_4' e identifique manualmente,"
    echo "depois: docker kill <container_do_lider>"
    exit 1
  fi
  echo "Lider identificado: $LEADER. Matando o container..."
  docker kill "$LEADER"
  echo "Aguarde ate ~10s: o monitor de heartbeat dos demais nos deve detectar a ausencia e disparar nova eleicao."
  exit 0
fi

if [ "$1" = "status" ]; then
  docker compose ps
  exit 0
fi

usage
