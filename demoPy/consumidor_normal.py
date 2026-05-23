import pika
import json

"""
Consumidor de Pedidos NORMALES - Python
=========================================
Se suscribe al exchange 'pedidos_exchange' con la routing key 'pedido.normal'.
Solo recibe y procesa pedidos de envio estandar (3-5 dias habiles).
"""

EXCHANGE_NAME = "pedidos_exchange"
QUEUE_NAME    = "cola_normal"
ROUTING_KEY   = "pedido.normal"


def procesar_pedido_normal(pedido: dict):
    """
    Simula el flujo estandar: verificacion de inventario,
    empaque y programacion de recogida con transportadora.
    """
    print(f"  -> [NORMAL] Verificando inventario para pedido #{pedido['id']}...")
    print(f"  -> [NORMAL] Empacando '{pedido['producto']}' (x{pedido['cantidad']})...")
    print(f"  -> [NORMAL] Programando recogida con transportadora (3-5 dias habiles)...")
    print(f"  -> [NORMAL] Numero de guia generado. HECHO.\n")


def callback(ch, method, properties, body):
    pedido = json.loads(body.decode("utf-8"))
    print(f"[ConsumidorNormal] Pedido recibido: {json.dumps(pedido)}")
    procesar_pedido_normal(pedido)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    channel.queue_declare(queue=QUEUE_NAME)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("[ConsumidorNormal] Esperando pedidos normales. CTRL+C para salir.")
    channel.start_consuming()


if __name__ == "__main__":
    main()
