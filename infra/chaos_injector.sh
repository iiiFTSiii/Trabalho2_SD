#!/bin/bash
set -e

if [ "$1" = "latency" ]; then
  echo "Aplicando latência e perda de pacotes..."
  docker exec --privileged sensor_1 tc qdisc add dev eth0 root netem delay 2000ms 1900ms distribution normal loss 5% || true
  docker exec --privileged sensor_2 tc qdisc add dev eth0 root netem delay 500ms 400ms distribution normal loss 5% || true
  exit 0
fi

if [ "$1" = "partition" ]; then
  echo "Simulando partição de rede entre os semáforos..."
  docker network disconnect sd_trabalho2_sd_network semaforo_1 || true
  docker network disconnect sd_trabalho2_sd_network semaforo_2 || true
  exit 0
fi

if [ "$1" = "restore" ]; then
  echo "Restaurando rede..."
  docker network connect sd_trabalho2_sd_network semaforo_1 || true
  docker network connect sd_trabalho2_sd_network semaforo_2 || true
  exit 0
fi

echo "Uso: $0 {latency|partition|restore}"