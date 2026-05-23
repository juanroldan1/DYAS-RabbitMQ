import pika
import json
from datetime import datetime

"""
Productor de Pedidos E-commerce - Python
=========================================
Caso de uso: sistema de e-commerce que enruta pedidos a diferentes
colas segun el tipo de envio mediante un exchange de tipo 'topic'.

Routing keys:
  pedido.express       -> entrega en 24h
  pedido.normal        -> entrega en 3-5 dias
  pedido.internacional -> entrega en 15-30 dias
"""

EXCHANGE_NAME = "pedidos_exchange"

def enviar_pedido(channel, routing_key, id_pedido, producto, cantidad, precio):
    """
    Publica un pedido en el exchange con la routing key indicada.
    El mensaje se serializa como JSON para que sea facil de parsear por el consumidor.
    """
    pedido = {
        "id": id_pedido,
        "producto": producto,
        "cantidad": cantidad,
        "precio": precio,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    # Serializar el pedido a JSON y codificarlo en bytes
    body = json.dumps(pedido).encode("utf-8")

    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=body
    )
    print(f"[Productor] Pedido enviado [{routing_key}] -> {json.dumps(pedido)}")


def main():
    # 1. Conectar a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()

    # 2. Declarar el exchange de tipo topic
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    # 3. Enviar pedidos de diferentes tipos
    enviar_pedido(channel, "pedido.express",       "2001", "Laptop Gamer",        2, 3500000)
    enviar_pedido(channel, "pedido.normal",        "2002", "Teclado Mecanico",    1,  250000)
    enviar_pedido(channel, "pedido.internacional", "2003", "Camara DSLR",         1, 4200000)
    enviar_pedido(channel, "pedido.express",       "2004", "Audifonos Bluetooth", 3,  180000)
    enviar_pedido(channel, "pedido.normal",        "2005", "Mouse Gamer",         2,  120000)

    print("\n[Productor] Todos los pedidos han sido enviados.")
    connection.close()


if __name__ == "__main__":
    main()
