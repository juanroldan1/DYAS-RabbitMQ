package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Productor de Pedidos E-commerce
 *
 * Caso de uso: Un sistema de e-commerce recibe pedidos y los enruta
 * a diferentes colas segun el tipo de envio usando un exchange de tipo "topic".
 *
 * Routing keys usadas:
 *   pedido.express       -> consumidor de pedidos express (envio en 24h)
 *   pedido.normal        -> consumidor de pedidos normales (envio en 3-5 dias)
 *   pedido.internacional -> consumidor de pedidos internacionales (envio en 15-30 dias)
 */
public class ProductorPedidos {

    // Nombre del exchange topic
    private static final String EXCHANGE_NAME = "pedidos_exchange";

    public static void main(String[] args) throws Exception {

        // 1. Crear la fabrica de conexiones y conectar a RabbitMQ local
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 2. Declarar el exchange de tipo "topic"
            //    topic permite filtrar mensajes por patron de routing key (e.g. pedido.*)
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            // 3. Simular el envio de pedidos de distintos tipos
            enviarPedido(channel, "pedido.express", "1001", "Laptop Gamer", 2, 3500000);
            enviarPedido(channel, "pedido.normal",  "1002", "Teclado Mecanico", 1, 250000);
            enviarPedido(channel, "pedido.internacional", "1003", "Camara DSLR", 1, 4200000);
            enviarPedido(channel, "pedido.express", "1004", "Audifonos Bluetooth", 3, 180000);
            enviarPedido(channel, "pedido.normal",  "1005", "Mouse Gamer", 2, 120000);

            System.out.println("\n[Productor] Todos los pedidos han sido enviados a la cola.");
        }
    }

    /**
     * Construye y publica un mensaje de pedido en el exchange.
     *
     * @param channel      Canal RabbitMQ activo
     * @param routingKey   Clave de enrutamiento (define el tipo de pedido)
     * @param idPedido     Identificador unico del pedido
     * @param producto     Nombre del producto
     * @param cantidad     Cantidad solicitada
     * @param precio       Precio unitario en pesos colombianos
     */
    private static void enviarPedido(Channel channel, String routingKey,
                                     String idPedido, String producto,
                                     int cantidad, long precio) throws Exception {

        // Timestamp del momento en que se genera el pedido
        String timestamp = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // Construir el mensaje como texto simple (JSON manual para no depender de librerias extra)
        String mensaje = String.format(
            "{\"id\":\"%s\",\"producto\":\"%s\",\"cantidad\":%d,\"precio\":%d,\"timestamp\":\"%s\"}",
            idPedido, producto, cantidad, precio, timestamp
        );

        // Publicar el mensaje en el exchange con la routing key correspondiente
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, mensaje.getBytes("UTF-8"));

        System.out.printf("[Productor] Pedido enviado [%s] -> %s%n", routingKey, mensaje);
    }
}
