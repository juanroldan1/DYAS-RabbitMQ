import pika
import json

"""
Consumidor de Pedidos EXPRESS - Python
========================================
Se suscribe al exchange 'pedidos_exchange' con la routing key 'pedido.express'.
Solo recibe y procesa pedidos marcados como express (entrega en 24h).
"""

EXCHANGE_NAME = "pedidos_exchange"
QUEUE_NAME    = "cola_express"
ROUTING_KEY   = "pedido.express"


def procesar_pedido_express(pedido: dict):
    """
    Simula el flujo de procesamiento de un pedido express:
    verificacion de pago, preparacion y asignacion de mensajero.
    """
    print(f"  -> [EXPRESS] Verificando pago del pedido #{pedido['id']}...")
    print(f"  -> [EXPRESS] Preparando '{pedido['producto']}' (x{pedido['cantidad']}) para envio en 24h...")
    print(f"  -> [EXPRESS] Asignando mensajero prioritario...")
    print(f"  -> [EXPRESS] Confirmacion enviada al cliente. HECHO.\n")


def callback(ch, method, properties, body):
    """Funcion de callback: se ejecuta cada vez que llega un mensaje a la cola."""
    pedido = json.loads(body.decode("utf-8"))
    print(f"[ConsumidorExpress] Pedido recibido: {json.dumps(pedido)}")
    procesar_pedido_express(pedido)


def main():
    # 1. Conectar a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    # 2. Declarar el exchange (debe coincidir con el productor)
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    # 3. Declarar la cola y vincularla al exchange con la routing key
    channel.queue_declare(queue=QUEUE_NAME)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

    # 4. Registrar el callback y empezar a consumir
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("[ConsumidorExpress] Esperando pedidos express. CTRL+C para salir.")
    channel.start_consuming()


if __name__ == "__main__":
    main()
