package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Consumidor de Pedidos EXPRESS
 *
 * Se suscribe unicamente a mensajes con routing key "pedido.express".
 * Simula el procesamiento de pedidos con envio en 24 horas.
 */
public class ConsumidorExpress {

    private static final String EXCHANGE_NAME = "pedidos_exchange";
    // Cola exclusiva para pedidos express
    private static final String QUEUE_NAME = "cola_express";
    // Patron que acepta solo pedidos express
    private static final String ROUTING_KEY = "pedido.express";

    public static void main(String[] args) throws Exception {

        // 1. Conexion a RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 2. Declarar el exchange (debe coincidir con el productor)
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // 3. Declarar la cola y vincularla al exchange con la routing key
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println("[ConsumidorExpress] Esperando pedidos express. CTRL+C para salir.");

        // 4. Callback: se ejecuta por cada mensaje recibido
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensaje = new String(delivery.getBody(), "UTF-8");
            System.out.println("\n[ConsumidorExpress] Pedido recibido: " + mensaje);
            // Simular el procesamiento del pedido express
            procesarPedidoExpress(mensaje);
        };

        // 5. Iniciar consumo (auto-ack = true para simplificar el ejemplo)
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    /**
     * Simula el flujo de procesamiento de un pedido express:
     * verificacion de pago, asignacion de mensajero y confirmacion.
     */
    private static void procesarPedidoExpress(String mensaje) {
        System.out.println("  -> [EXPRESS] Verificando pago...");
        System.out.println("  -> [EXPRESS] Preparando paquete para envio en 24h...");
        System.out.println("  -> [EXPRESS] Asignando mensajero prioritario...");
        System.out.println("  -> [EXPRESS] Confirmacion enviada al cliente. HECHO.");
    }
}
