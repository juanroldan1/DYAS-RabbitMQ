import pika
import json

"""
Consumidor de Pedidos INTERNACIONALES - Python
================================================
Se suscribe al exchange 'pedidos_exchange' con la routing key 'pedido.internacional'.
Gestiona el flujo aduanero y logistico de envios al exterior (15-30 dias).
"""

EXCHANGE_NAME = "pedidos_exchange"
QUEUE_NAME    = "cola_internacional"
ROUTING_KEY   = "pedido.internacional"


def procesar_pedido_internacional(pedido: dict):
    """
    Simula el flujo internacional: revision aduanal,
    calculo de aranceles y asignacion de operador logistico.
    """
    print(f"  -> [INTERNACIONAL] Revisando restricciones de exportacion para pedido #{pedido['id']}...")
    print(f"  -> [INTERNACIONAL] Generando declaracion aduanal para '{pedido['producto']}'...")
    print(f"  -> [INTERNACIONAL] Calculando aranceles e impuestos...")
    print(f"  -> [INTERNACIONAL] Contratando operador logistico internacional...")
    print(f"  -> [INTERNACIONAL] Entrega estimada: 15-30 dias. HECHO.\n")


def callback(ch, method, properties, body):
    pedido = json.loads(body.decode("utf-8"))
    print(f"[ConsumidorInternacional] Pedido recibido: {json.dumps(pedido)}")
    procesar_pedido_internacional(pedido)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    channel.queue_declare(queue=QUEUE_NAME)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("[ConsumidorInternacional] Esperando pedidos internacionales. CTRL+C para salir.")
    channel.start_consuming()


if __name__ == "__main__":
    main()
