import os, time, json
from fastapi import FastAPI, Response
import pika

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
params = pika.URLParameters(RABBIT_URL)

app = FastAPI(title="Before/After RabbitMQ")

def heavy_work(ms: int = 150):
    time.sleep(ms/1000)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/sync")
def sync(ms: int = 150):
    heavy_work(ms)
    return {"status": "done-sync", "ms": ms}

@app.get("/async")
def enqueue(ms: int = 150):
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="jobs", durable=True)
        body = json.dumps({"ms": ms}).encode()
        channel.basic_publish(
            exchange="",
            routing_key="jobs",
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        connection.close()
        return Response(content='{"status":"queued"}', media_type="application/json", status_code=202)
    except Exception:
        # Broker no disponible (evita tumbar el API)
        return Response(content='{"error":"broker_unavailable"}', media_type="application/json", status_code=503)
