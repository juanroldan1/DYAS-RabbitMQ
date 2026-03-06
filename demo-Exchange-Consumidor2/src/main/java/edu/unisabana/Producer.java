package edu.unisabana;

import com.rabbitmq.client.ConnectionFactory;

import java.time.format.DateTimeFormatter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Hello world!
 *
 */
public class Producer {
    /**
     * @param args
     */
    public static void main(String[] args) {
        // crear una facbrica de conexiones
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        // factory.setUsername("guest");
        // factory.setPassword("guest");

        try {
            // crear un conexion y un canal para rabbitMQ
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // crear un exchange de tipo fanout
            String exchangeName = "ARQ_SOFT_EXCHANGE";
            channel.exchangeDeclare(exchangeName, "fanout");

            String queue1 = "ARQ_SOFT_1";
            String queue2 = "ARQ_SOFT_2";

            // declarar una cola
            channel.queueDeclare(queue1, false, false, false, null);
            channel.queueDeclare(queue2, false, false, false, null);

            // vincular la cola al exchange
            channel.queueBind(queue1, exchangeName, "");
            channel.queueBind(queue2, exchangeName, "");

            // Crear un timestamp para agregarlo al mensaje
            DateTimeFormatter timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String timestampStr = timestamp.format(java.time.LocalDateTime.now());

            // publicar un mensaje
            String message = "Hello, RabbitMQ! - " + timestampStr;
            channel.basicPublish(exchangeName, "", null, message.getBytes());

            // Cerrar el canal y la conexion
            System.out.println(" [x] Sent '" + message + "'");
            channel.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
