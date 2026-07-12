FROM python:3.9-slim
RUN apt-get update && apt-get install -y iproute2 iptables
WORKDIR /app
COPY sensor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sensor/ ./sensor/
COPY comum/ ./comum/
ENV PYTHONPATH="${PYTHONPATH}:/app"
CMD ["python", "sensor/main.py"]