import os, time, json
import pika

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "4"))

def heavy_work(ms: int = 150):
    time.sleep(ms/1000)

def connect_with_retry(url: str, retry_sec: int = 2):
    params = pika.URLParameters(url)
    while True:
        try:
            conn = pika.BlockingConnection(params)
            return conn
        except Exception as e:
            print(f"[worker] Broker no disponible: {e}. Reintentando en {retry_sec}s ...")
            time.sleep(retry_sec)

def main():
    connection = connect_with_retry(RABBIT_URL)
    channel = connection.channel()
    channel.queue_declare(queue="jobs", durable=True)
    # Prefetch igual a la concurrencia para no sobrecargar un solo worker
    channel.basic_qos(prefetch_count=CONCURRENCY)

    def on_message(ch, method, properties, body):
        try:
            job = json.loads(body.decode())
            ms = int(job.get("ms", 150))
            heavy_work(ms)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print("[worker] Error procesando mensaje:", e)
            # DLQ real: usar exchange/cola de muertos; aquí lo descartamos
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue="jobs", on_message_callback=on_message)
    print(f"[worker] Iniciado. Concurrency lógica: {CONCURRENCY}")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("[worker] Saliendo…")
    finally:
        try:
            connection.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
