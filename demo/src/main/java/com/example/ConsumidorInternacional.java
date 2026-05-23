package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Consumidor de Pedidos INTERNACIONALES
 *
 * Se suscribe a mensajes con routing key "pedido.internacional".
 * Simula el flujo aduanero y logistico de envios al exterior (15-30 dias).
 */
public class ConsumidorInternacional {

    private static final String EXCHANGE_NAME = "pedidos_exchange";
    private static final String QUEUE_NAME = "cola_internacional";
    private static final String ROUTING_KEY = "pedido.internacional";

    public static void main(String[] args) throws Exception {

        // 1. Conexion a RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 2. Declarar exchange topic
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // 3. Declarar cola y binding con routing key internacional
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println("[ConsumidorInternacional] Esperando pedidos internacionales. CTRL+C para salir.");

        // 4. Callback de procesamiento
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensaje = new String(delivery.getBody(), "UTF-8");
            System.out.println("\n[ConsumidorInternacional] Pedido recibido: " + mensaje);
            procesarPedidoInternacional(mensaje);
        };

        // 5. Iniciar consumo
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    /**
     * Simula el flujo internacional: revision de restricciones de exportacion,
     * declaracion aduanal y contratacion de operador logistico internacional.
     */
    private static void procesarPedidoInternacional(String mensaje) {
        System.out.println("  -> [INTERNACIONAL] Revisando restricciones de exportacion...");
        System.out.println("  -> [INTERNACIONAL] Generando declaracion aduanal...");
        System.out.println("  -> [INTERNACIONAL] Calculando aranceles e impuestos...");
        System.out.println("  -> [INTERNACIONAL] Contratando operador logistico internacional...");
        System.out.println("  -> [INTERNACIONAL] Tiempo estimado de entrega: 15-30 dias. HECHO.");
    }
}
