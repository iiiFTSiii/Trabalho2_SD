# Sistema Distribuído de Monitoramento de Tráfego Urbano

Este projeto implementa um protótipo distribuído descentralizado para monitoramento e controle de tráfego urbano em tempo real usando um modelo baseado em eventos com RabbitMQ. A arquitetura é composta por:

- sensores que publicam dados de fluxo de veículos;
- semáforos que consomem esses dados e processam de forma causal;
- um broker RabbitMQ para comunicação;
- mecanismos básicos de eleição de líder, quorum, checkpoint e injeção de caos.

## Como o projeto funciona

### 1. Sensores
Os containers `sensor_1` e `sensor_2` simulam nós sensores. Eles:

- publicam mensagens com dados como `sensor_id`, `cars`, `lamport_time` e `physical_time`;
- usam um relógio lógico de Lamport para ordenar eventos localmente;
- enviam as mensagens para o exchange `traffic_data` do RabbitMQ.

### 2. Semáforos
Os containers `semaforo_1` e `semaforo_2` simulam atuadores. Eles:

- recebem as mensagens publicadas pelos sensores;
- armazenam mensagens fora de ordem quando necessário;
- processam as mensagens de forma causal com base no Lamport;
- realizam uma eleição simples de líder com verificação de quorum.

### 3. Tolerância a falhas e recuperação
O sistema possui:

- checkpoint simples para persistir o estado do semáforo;
- recuperação de estado ao reiniciar o container;
- heartbeats básicos para monitorar nós ativos.

### 4. Injeção de caos
O script [infra/chaos_injector.sh](infra/chaos_injector.sh) permite simular cenários de falha e rede para validar o comportamento do sistema:

- latência variável e perda de pacotes;
- partição de rede entre os semáforos;
- restauração da rede.

## Como rodar o projeto

Na raiz do projeto, execute:

```bash
docker compose up --build
```

Esse comando sobe:

- o broker RabbitMQ;
- os sensores;
- os semáforos.

Se os containers já foram construídos antes, você pode usar:

```bash
docker compose up
```

## Como verificar se o sistema está funcionando

### 1. Verificar o estado dos containers

```bash
docker compose ps
```

Você deve ver os containers com status `Up`.

### 2. Acompanhar os logs

```bash
docker compose logs -f sensor_1 sensor_2 semaforo_1 semaforo_2
```

O que é esperado aparecer:

- nos sensores:
  - mensagens de publicação;
  - logs indicando o tempo inicial de deriva do relógio local.
- nos semáforos:
  - logs de recebimento das mensagens;
  - logs de processamento causal;
  - logs de eleição de líder ou modo seguro.

## Como testar a injeção de caos

### Latência e perda de pacotes

```bash
bash infra/chaos_injector.sh latency
```

Esse comando aplica latência e perda de pacotes nos sensores para simular uma rede não confiável.

### Partição de rede

```bash
bash infra/chaos_injector.sh partition
```

Esse comando simula uma partição de rede entre os semáforos.

### Restaurar a rede

```bash
bash infra/chaos_injector.sh restore
```

Esse comando restaura a conectividade da rede.

## Como parar o sistema

```bash
docker compose down
```
