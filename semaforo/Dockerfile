FROM python:3.9-slim

# Instala ferramentas de rede (iproute2 e iptables) necessárias para a injeção de caos
RUN apt-get update && apt-get install -y iproute2 iptables

WORKDIR /app

# Copia e instala as dependências
COPY semaforo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do semáforo e a pasta comum (onde está a lógica de falhas)
COPY semaforo/ ./semaforo/
COPY comum/ ./comum/

ENV PYTHONPATH="${PYTHONPATH}:/app"

CMD ["python", "semaforo/main.py"]