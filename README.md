# Sistema Distribuído de Monitoramento de Tráfego Urbano (SD-Tráfego)

Protótipo distribuído descentralizado para monitoramento e controle de tráfego
urbano em tempo real, usando arquitetura orientada a eventos (Publish-Subscribe)
com RabbitMQ.

## Arquitetura

- **2 sensores** (`sensor_1`, `sensor_2`): publicam dados de fluxo de veículos
  com relógio lógico de Lamport e relógio físico com deriva artificial,
  sincronizado periodicamente via Algoritmo de Cristian.
- **4 semáforos** (`semaforo_1..4`): consomem os dados, reordenam causalmente,
  fazem heartbeat entre si, elegem um líder (variante do Bully com quórum de
  maioria estrita) e persistem checkpoints em volume próprio.
- **1 broker RabbitMQ** central.

Usar 4 semáforos (em vez de 2) é o que permite demonstrar de verdade o cenário
de partição 2x2 exigido no enunciado (Restrição B) sem risco de split-brain.

## Métodos utilizados para garantir tolerancia a falhas:

1. **Sincronização de relógio real (Cristian)**: os sensores agora fazem uma
   requisição/resposta via RabbitMQ (`time_sync_request`) para o semáforo
   líder (que não sofre deriva), medem o RTT e calculam o offset pelo
   Algoritmo de Cristian de fato.
2. **Heartbeat conectado à eleição**: antes a classe `HeartbeatMonitor` existia mas nunca era instanciada; o quórum nunca era atingido.
3. **Eleição reativa**: monitor em background detecta quando o líder para de mandar heartbeat
   (`docker kill`, queda de rede) e dispara uma nova eleição automaticamente.
4. **Buffer causal sem risco de travar para sempre**: buffer é liberado por uma janela de tempo e ordenado por
   `(lamport_time, sensor_id)`, com desempate determinístico.
5. **Persistência real de checkpoint**: cada semáforo tem um volume
   Docker nomeado próprio (`semaforo_N_data`), então o estado sobrevive a um
   `docker kill` + reinício do mesmo container.
6. **Partição 2x2 de verdade**: o script de caos agora isola `semaforo_3` e
   `semaforo_4` da rede, deixando `semaforo_1` e `semaforo_2` do outro lado —
   com quórum = 3/4, nenhum dos dois lados consegue eleger líder sozinho.

## Como rodar

```bash
docker compose up --build
```

Isso sobe: RabbitMQ, 2 sensores e 4 semáforos.

## Verificando o sistema

```bash
docker compose ps
docker compose logs -f semaforo_1 semaforo_2 semaforo_3 semaforo_4
```

Nos logs dos semáforos você deve ver:
- `Iniciando eleicao...` / `Eu sou o novo lider` / `Novo lider e semaforo_X`
- `Processando causalmente (#N): {...}` — mensagens já reordenadas
- `Sincronizado via Cristian: offset=... rtt=...` (nos logs dos sensores)

## Cenários de caos (`infra/chaos_injector.sh`)

```bash
# Restrição A: latência flutuante + 5% de perda nos sensores
bash infra/chaos_injector.sh latency

# Restrição B: partição 2x2 entre os semáforos (nenhum lado deve eleger líder)
bash infra/chaos_injector.sh partition

# Restaurar a rede (deve ocorrer nova eleição automaticamente, quórum 4/4)
bash infra/chaos_injector.sh restore

# Matar o líder atual e observar a reeleição automática
bash infra/chaos_injector.sh kill-leader

# Ver status dos containers
bash infra/chaos_injector.sh status
```

Para provar a recuperação de estado (Restrição/Módulo 4), depois do
`kill-leader` (ou de um `docker kill semaforo_X` manual), religue o container
com `docker start semaforo_X` (não recrie com `up`, senão perde o propósito de
mostrar continuidade) e confira nos logs a linha `Recuperacao concluida.
Mensagens ja processadas antes da queda: N`.

## Parar o sistema

```bash
docker compose down
```

Para apagar também os volumes de checkpoint:

```bash
docker compose down -v
```