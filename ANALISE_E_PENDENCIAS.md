# Análise do projeto contra os requisitos

## Resumo executivo

O projeto recebeu uma primeira rodada de implementação para cobrir os requisitos de alta e média prioridade descritos em [Requisitos.txt](Requisitos.txt). A infraestrutura Docker foi alinhada aos arquivos reais, a base do fluxo de publicação/consumo foi estabilizada, a ordenação causal passou a ser tratada de forma explícita, a eleição de líder passou a considerar quorum absoluto, a recuperação de checkpoints foi tornada compatível com testes e execução local e o script de caos ganhou suporte para simular latência, perda de pacotes e partição de rede.

Os testes básicos do projeto foram executados com sucesso e a sintaxe do compose também foi validada.

---

## O que já existe

### Estrutura inicial do sistema
- Pasta para sensores e semáforos.
- Arquivo de composição Docker com RabbitMQ e serviços de sensor/semaforo.
- Módulos Python para:
  - relógio lógico;
  - sincronização de relógios (esboço);
  - eleição de líder (esboço);
  - checkpoint e recuperação (esboço);
  - script de caos básico.

### Implementações parciais
- Publicação de mensagens com campo de Lamport.
- Consumo e reordenação simples por Lamport.
- Checkpoint em arquivo JSON.
- Script simples de injeção de latência/perda.

---

## Implementações realizadas

### 1. Infraestrutura e execução dos containers

- Os caminhos do docker-compose foram corrigidos para apontar para os Dockerfiles reais de sensor e semáforo.
- Os entrypoints do Dockerfile foram ajustados para os arquivos Python corretos.
- A estrutura passou a ser compatível com a execução local e com a construção de containers.

### 2. Pub/Sub e ordenação causal

- O módulo de sensores passou a publicar mensagens com Lamport e metadados de tempo físico.
- O consumidor passou a armazenar mensagens fora de ordem e processá-las de acordo com a ordenação causal.
- O fluxo ganhou logs mais claros para demonstrar a sequência lógica de processamento.

### 3. Eleição de líder e quorum absoluto

- A lógica de eleição foi reorganizada em uma implementação mais simples e verificável.
- O projeto passou a avaliar quorum absoluto com base em mais de 50% dos nós totais.
- O sistema agora entra em modo seguro quando não há quorum suficiente.

### 4. Sincronização de relógio físico

- O módulo de sincronização de relógio foi mantido e integrado ao fluxo de publicação com um offset simples baseado em Cristian.
- Isso dá base para a demonstração do conceito em ambiente distribuído.

### 5. Tolerância a falhas e recuperação

- O checkpoint foi reestruturado em um módulo próprio e passou a aceitar path configurável.
- A recuperação de estado foi integrada ao fluxo do semáforo para restaurar o Lamport esperado após reinicialização.
- O semáforo ganhou uma base de heartbeats para monitorar nós ativos e apoiar a detecção de falhas.

### 6. Testes básicos

- Foram criados testes unitários para:
  - relógio de Lamport;
  - sincronização de relógio;
  - checkpoint e recuperação;
  - avaliação de quorum.

---

## Problemas críticos identificados

### 1. Infraestrutura e execução dos containers

#### Problemas
- O arquivo docker-compose referencia Dockerfiles e caminhos que não existem:
  - `sensores_publishers/Dockerfile`
  - `semaforos_subscribers/Dockerfile`
- Os Dockerfiles esperam comandos de entrada que não existem:
  - `sensor/main_sensor.py`
  - `semaforo/main_semaforo.py`
- Os arquivos reais do projeto são:
  - [sensor/main.py](sensor/main.py)
  - [semaforo/main.py](semaforo/main.py)

#### O que precisa mudar
- Corrigir os caminhos dos Dockerfiles no docker-compose.
- Ajustar o `CMD` dos Dockerfiles para os arquivos reais.
- Verificar se todos os módulos são copiados corretamente para o container.
- Garantir que o ambiente Python tenha o `PYTHONPATH` correto.

---

### 2. Pub/Sub e ordenação causal

#### Problemas
- O sistema publica mensagens com Lamport, mas a implementação ainda é muito simples.
- A reordenação no consumidor usa apenas uma fila local e processa mensagens de forma básica.
- Não há prova robusta de que a ordem causal é preservada sob atrasos extremos e recepção fora de ordem.
- Não há uso explícito de relógios vetoriais nem de uma estratégia mais forte para lidar com mensagens causais em ambientes com perda e atraso.

#### O que precisa mudar
- Implementar um mecanismo mais sólido de bufferização e reordenação causal.
- Garantir que mensagens recebidas fora de ordem sejam armazenadas até que todas as dependências causais sejam satisfeitas.
- Exibir logs claros que comprovem a ordenação lógica.
- Considerar a possibilidade de usar relógios vetoriais, se o grupo quiser uma solução mais explícita para a restrição A.

---

### 3. Eleição de líder e quorum absoluto

#### Problemas
- A implementação atual de eleição não representa de forma realista um algoritmo distribuído.
- Ela não recebe de forma consistente mensagens de outros nós.
- Não há heartbeat real nem detecção de falhas entre os semáforos.
- O mecanismo de quorum é muito simplificado e não garante proteção contra split-brain em cenários reais de partição.

#### O que precisa mudar
- Implementar um algoritmo de eleição mais completo, com troca real de mensagens entre os nós.
- Definir claramente o conjunto de nós ativos e o quórum mínimo (`> 50%` do total de nós).
- Garantir que, em caso de partição, a subrede que não atingir o quorum entre em modo seguro e não eleja líder.
- Adicionar logs de eleição, liderança atual, falha detectada e transição para segurança.

---

### 4. Sincronização de relógio físico

#### Problemas
- O arquivo de sincronização existe, mas não é integrado de forma real à execução do sistema.
- O sensor apenas introduz uma deriva artificial de tempo local sem qualquer mecanismo distribuído de correção.
- Não há troca de tempo entre nós para calcular offset via um algoritmo semelhante a Cristian ou Berkeley.

#### O que precisa mudar
- Implementar um mecanismo distribuído de sincronização de relógio usando o próprio middleware Pub/Sub.
- Cada nó deve calcular um offset com base em mensagens de sincronização e RTT estimado.
- O relógio físico usado para relatórios deve refletir a correção distribuída.

---

### 5. Tolerância a falhas e recuperação

#### Problemas
- O checkpoint existe, mas é muito básico.
- O estado é salvo em um caminho fixo (`/app/state.json`) sem garantir persistência forte em reinicializações ou retomadas.
- Não há histórico global consistente nem recuperação transparente de falhas com preservação de eventos já processados.
- Não há demonstração real de reinicialização após `docker kill`.

#### O que precisa mudar
- Definir um armazenamento persistente por nó, idealmente com volume Docker.
- Implementar recuperação do estado em reinicialização do container.
- Garantir que o processo recomece a partir do último ponto seguro.
- Adicionar logs e testes de falha para demonstrar recuperação.

---

### 6. Injeção de caos e validação

#### Problemas
- O script atual apenas aplica latência e perda de pacotes em alguns containers.
- Não há script para partição de rede, que é um requisito obrigatório da restrição B.
- O projeto ainda não possui uma forma prática de validar os três cenários obrigatórios em execução.

#### O que precisa mudar
- Expandir o script de caos para:
  - injetar latência variável;
  - introduzir perda de pacotes;
  - simular partições de rede entre subconjuntos de nós;
  - aplicar comportamento controlado de falha.
- Criar um fluxo de validação para demonstrar, em tempo real, que:
  - mensagens foram reordenadas causalmente;
  - não houve múltiplos líderes em partição;
  - os relógios foram sincronizados.

---

## O que falta fazer para o projeto ficar próximo do requisito completo

### Prioridade alta
- [x] Corrigir o docker-compose e os Dockerfiles para que os containers subam corretamente.
- [x] Ajustar os entrypoints para os arquivos Python corretos.
- [x] Implementar um fluxo básico de publicação/consumo com RabbitMQ.
- [x] Implementar ordenação causal simples para as mensagens.
- [x] Implementar eleição de líder com quorum absoluto e modo seguro.
- [x] Implementar sincronização de relógio com base em Cristian.

### Prioridade média
- [x] Implementar recuperação de estado após reinicialização.
- [x] Adicionar heartbeats e detecção de nós indisponíveis.
- [x] Criar script de caos com partição de rede e falha controlada.
- [x] Melhorar logs para demonstração no seminário.

### Prioridade baixa
- [ ] Documentar o fluxo de execução para o professor.
- [ ] Preparar uma demonstração passo a passo dos cenários de validação.
- [ ] Revisar a arquitetura para separar melhor os papéis de cada módulo.

---

## Conclusão

A base do projeto foi significativamente avançada e agora já apresenta uma implementação funcional mínima dos requisitos de alta e média prioridade. O sistema está em condições de ser executado localmente, de publicar mensagens com ordenação causal, de eleger líder com quorum, de restaurar estado e de simular cenários básicos de caos.

O próximo passo natural é aprofundar a sincronização distribuída de relógio físico e preparar uma demonstração mais completa para o seminário.
