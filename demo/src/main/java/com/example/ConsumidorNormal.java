package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Consumidor de Pedidos NORMALES
 *
 * Se suscribe unicamente a mensajes con routing key "pedido.normal".
 * Simula el procesamiento de pedidos con envio estandar de 3 a 5 dias habiles.
 */
public class ConsumidorNormal {

    private static final String EXCHANGE_NAME = "pedidos_exchange";
    private static final String QUEUE_NAME = "cola_normal";
    private static final String ROUTING_KEY = "pedido.normal";

    public static void main(String[] args) throws Exception {

        // 1. Conexion a RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 2. Declarar el exchange topic (igual que el productor)
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // 3. Declarar cola y enlazarla con la routing key de pedidos normales
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println("[ConsumidorNormal] Esperando pedidos normales. CTRL+C para salir.");

        // 4. Callback de procesamiento
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensaje = new String(delivery.getBody(), "UTF-8");
            System.out.println("\n[ConsumidorNormal] Pedido recibido: " + mensaje);
            procesarPedidoNormal(mensaje);
        };

        // 5. Iniciar consumo
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    /**
     * Simula el flujo estandar: verificacion, empaque y despacho regular.
     */
    private static void procesarPedidoNormal(String mensaje) {
        System.out.println("  -> [NORMAL] Verificando disponibilidad en inventario...");
        System.out.println("  -> [NORMAL] Empacando producto...");
        System.out.println("  -> [NORMAL] Programando recogida con transportadora (3-5 dias)...");
        System.out.println("  -> [NORMAL] Numero de guia generado. HECHO.");
    }
}
