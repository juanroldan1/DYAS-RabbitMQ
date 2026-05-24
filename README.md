### Taller sobre RabbitMQ

> **Equipo:**
>
> - Brayan Presiga Sepulveda - 0000301424
> - Juan David Sanchez Roldan - 0000340321
> - Yuly Dayana Rodríguez Salcedo - 0000305314

---

#### Objetivo

El objetivo de este taller es aprender a utilizar RabbitMQ para la mensajería asincrónica en aplicaciones. Implementaremos ejemplos prácticos para entender cómo enviar y recibir mensajes utilizando RabbitMQ.

#### Pre-requisitos

- Conocimientos básicos de Java o Python.
- Familiaridad con conceptos de mensajería y cola de mensajes.
- Tener RabbitMQ instalado y en ejecución en tu máquina.

---

### Conceptos Clave

Antes de comenzar, es importante entender los componentes principales de RabbitMQ:

| Concepto        | Descripción                                                                             |
| --------------- | --------------------------------------------------------------------------------------- |
| **Producer**    | Aplicación que publica mensajes en RabbitMQ                                             |
| **Queue**       | Buffer donde se almacenan los mensajes hasta ser consumidos                             |
| **Consumer**    | Aplicación que lee y procesa mensajes de una cola                                       |
| **Exchange**    | Componente que recibe mensajes del producer y los enruta a las colas según reglas       |
| **Binding**     | Relación entre un exchange y una cola, definida por una routing key                     |
| **Routing Key** | Etiqueta que usa el exchange para decidir a qué cola enviar un mensaje                  |
| **Channel**     | Canal virtual dentro de una conexión TCP; reduce el costo de abrir múltiples conexiones |

**Flujo general:**

```
Producer --> Exchange --> (Binding / Routing Key) --> Queue --> Consumer
```

---

### Instalación de RabbitMQ

1. **Instalar RabbitMQ:**
   - Descarga e instala RabbitMQ desde [aquí](https://www.rabbitmq.com/download.html).

2. **Verificar que RabbitMQ esté en Ejecución:**
   - Asegúrate de que RabbitMQ esté en ejecución. Puedes verificarlo abriendo el panel de control de RabbitMQ en tu navegador web:
     - Por defecto, el panel de control de RabbitMQ está disponible en [http://localhost:15672](http://localhost:15672).
   - Inicia RabbitMQ desde la línea de comandos:
     ```sh
     rabbitmq-server
     ```

---

### Paso 1: Productor y Consumidor Básico en Java (`demo/`)

#### Descripción

Primera aproximación a RabbitMQ: un productor publica un mensaje `"Hello, RabbitMQ!"` en la cola `hello` y un consumidor lo lee e imprime en consola.

#### Configuración del `pom.xml`

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" ...>
    <groupId>com.example</groupId>
    <artifactId>demo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>com.rabbitmq</groupId>
            <artifactId>amqp-client</artifactId>
            <version>5.13.1</version>
        </dependency>
    </dependencies>
</project>
```

#### Productor en Java

```java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer {
    public static void main(String[] args) {
        // Crear la fábrica de conexiones apuntando a localhost
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Declarar la cola "hello" (se crea si no existe)
            channel.queueDeclare("hello", false, false, false, null);

            // Publicar el mensaje usando el exchange default ("")
            String message = "Hello, RabbitMQ!";
            channel.basicPublish("", "hello", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            // Cerrar canal y conexión explícitamente
            channel.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

#### Consumidor en Java

```java
import com.rabbitmq.client.*;

public class Consumer {
    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare("hello", false, false, false, null);

            // DeliverCallback: se ejecuta por cada mensaje recibido
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            };

            // auto_ack = true: reconocimiento automático del mensaje
            channel.basicConsume("hello", true, deliverCallback, consumerTag -> {});
            System.out.println(" [*] Waiting for messages. To exit press Ctrl+C");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

**Diagrama de flujo:**

```
Producer ──basicPublish──> [Exchange default ""] ──> [cola: hello] ──> Consumer
```

#### Cómo Ejecutar

1. Iniciar RabbitMQ: `rabbitmq-server`
2. Compilar: `mvn compile`
3. Terminal 1 — consumidor: `mvn exec:java -Dexec.mainClass="com.example.Consumer"`
4. Terminal 2 — productor: `mvn exec:java -Dexec.mainClass="com.example.Producer"`

---

### Paso 2: Productor y Consumidor Básico en Python (`demoPy/`)

#### Descripción

Equivalente del paso anterior implementado en Python usando la librería **pika**.

#### Configuración del entorno

```sh
pip install pika
```

#### Productor en Python

```python
import pika

def main():
    # Conectar con RabbitMQ en localhost
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declarar la cola (se crea si no existe)
    channel.queue_declare(queue='hello')

    # Publicar con exchange vacío (exchange default)
    channel.basic_publish(exchange='', routing_key='hello', body='Hello RabbitMQ!')
    print(" [x] Sent 'Hello RabbitMQ!'")

    # Cerrar la conexión
    connection.close()

if __name__ == '__main__':
    main()
```

#### Consumidor en Python

```python
import pika

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')

    # Callback: se ejecuta por cada mensaje recibido
    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")

    # auto_ack=True: reconocimiento automático
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()
```

#### Cómo Ejecutar

```sh
# Terminal 1 — consumidor
python Consumer.py

# Terminal 2 — productor
python Producer.py
```

---

### Paso 3: Exchange Fanout con 2 Consumidores en Java (`demo-Exchange-Consumidor2/`)

#### Descripción

Introduce el **exchange de tipo fanout**: un único mensaje publicado por el productor es retransmitido simultáneamente a **todas** las colas vinculadas, sin importar la routing key.

#### Diferencia clave respecto al Paso 1

| Aspecto                 | Simple Queue    | Fanout Exchange          |
| ----------------------- | --------------- | ------------------------ |
| Número de colas destino | 1               | N (todas las vinculadas) |
| Routing key             | Requerida       | Ignorada                 |
| Caso de uso principal   | Cola de trabajo | Broadcast / pub-sub      |

#### Productor en Java

```java
public class Producer {
    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Declarar el exchange de tipo fanout
            String exchangeName = "ARQ_SOFT_EXCHANGE";
            channel.exchangeDeclare(exchangeName, "fanout");

            // Declarar ambas colas
            channel.queueDeclare("ARQ_SOFT_1", false, false, false, null);
            channel.queueDeclare("ARQ_SOFT_2", false, false, false, null);

            // Vincular cada cola al exchange (routing key vacía: se ignora en fanout)
            channel.queueBind("ARQ_SOFT_1", exchangeName, "");
            channel.queueBind("ARQ_SOFT_2", exchangeName, "");

            // Publicar — el exchange retransmite a TODAS las colas vinculadas
            String message = "Hello, RabbitMQ! - " + timestamp;
            channel.basicPublish(exchangeName, "", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            channel.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

#### Consumidor 1 en Java (`Comsumer.java`)

```java
public class Comsumer {
    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Se suscribe a ARQ_SOFT_1
            channel.queueDeclare("ARQ_SOFT_1", false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            };

            channel.basicConsume("ARQ_SOFT_1", true, deliverCallback, consumerTag -> {});
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

#### Consumidor 2 en Java (`Comsumer2.java`)

Idéntico al consumidor 1, pero suscrito a `ARQ_SOFT_2`.

**Diagrama de flujo:**

```
Producer ──> [Exchange: ARQ_SOFT_EXCHANGE (fanout)]
                    ├──> [cola: ARQ_SOFT_1] ──> Consumer1
                    └──> [cola: ARQ_SOFT_2] ──> Consumer2
```

#### Cómo Ejecutar

```sh
mvn compile

# Terminal 1
mvn exec:java -Dexec.mainClass="edu.unisabana.Comsumer"

# Terminal 2
mvn exec:java -Dexec.mainClass="edu.unisabana.Comsumer2"

# Terminal 3 — el mensaje llega a los dos consumidores al mismo tiempo
mvn exec:java -Dexec.mainClass="edu.unisabana.Producer"
```

---

### Paso 4: API REST + Workers con Docker (`demo-API-Worker/`)

#### Descripción

Demuestra el patrón **API ↔ Worker** (Work Queue): una API REST recibe peticiones HTTP, las encola en RabbitMQ y responde `202 Accepted` de inmediato. Workers independientes consumen y procesan los trabajos en paralelo.

**Problema que resuelve:** sin colas, el endpoint `/sync` bloquea el servidor durante todo el tiempo de procesamiento. Con colas, el endpoint `/async` retorna en microsegundos y delega el trabajo pesado a los workers.

#### Arquitectura

```
Cliente HTTP ──GET /async──> [FastAPI : 8000] ──basicPublish──> [cola: jobs (durable)]
                                                                        │
                                        ┌───────────────────────────────┘
                                        ▼
                              [Worker 1] [Worker 2] [Worker 3]   ← 3 réplicas Docker
```

#### `api/api.py`

```python
import os, time, json
from fastapi import FastAPI, Response
import pika

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
params = pika.URLParameters(RABBIT_URL)
app = FastAPI(title="Before/After RabbitMQ")

def heavy_work(ms: int = 150):
    time.sleep(ms/1000)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/sync")
def sync(ms: int = 150):
    # Procesamiento bloqueante — el servidor no puede atender otras peticiones
    heavy_work(ms)
    return {"status": "done-sync", "ms": ms}

@app.get("/async")
def enqueue(ms: int = 150):
    # Encola el trabajo y retorna inmediatamente
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="jobs", durable=True)   # durable: sobrevive reinicios
        body = json.dumps({"ms": ms}).encode()
        channel.basic_publish(
            exchange="",
            routing_key="jobs",
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),  # persiste en disco
        )
        connection.close()
        return Response(content='{"status":"queued"}', media_type="application/json", status_code=202)
    except Exception:
        return Response(content='{"error":"broker_unavailable"}', media_type="application/json", status_code=503)
```

#### `worker/worker.py`

```python
import os, time, json
import pika

RABBIT_URL  = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "4"))

def heavy_work(ms: int = 150):
    time.sleep(ms/1000)

def connect_with_retry(url: str, retry_sec: int = 2):
    """Reintenta la conexión al broker hasta que esté disponible."""
    params = pika.URLParameters(url)
    while True:
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            print(f"[worker] Broker no disponible: {e}. Reintentando en {retry_sec}s ...")
            time.sleep(retry_sec)

def main():
    connection = connect_with_retry(RABBIT_URL)
    channel = connection.channel()
    channel.queue_declare(queue="jobs", durable=True)

    # prefetch_count: cuántos mensajes puede tener el worker sin hacer ack
    channel.basic_qos(prefetch_count=CONCURRENCY)

    def on_message(ch, method, properties, body):
        try:
            job = json.loads(body.decode())
            heavy_work(int(job.get("ms", 150)))
            ch.basic_ack(delivery_tag=method.delivery_tag)   # confirmar procesamiento OK
        except Exception as e:
            print("[worker] Error:", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # descartar si falla

    channel.basic_consume(queue="jobs", on_message_callback=on_message)
    print(f"[worker] Iniciado. Concurrencia lógica: {CONCURRENCY}")
    channel.start_consuming()

if __name__ == "__main__":
    main()
```

#### `docker-compose.yml`

```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    ports:
      - "5672:5672"
      - "15672:15672" # UI: http://localhost:15672 (guest/guest)
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_running"]
      interval: 5s
      timeout: 3s
      retries: 10

  api:
    build:
      context: ./api
    ports:
      - "8000:8000"
    environment:
      - RABBIT_URL=amqp://guest:guest@rabbitmq:5672/
    depends_on:
      rabbitmq:
        condition: service_healthy

  worker:
    build:
      context: ./worker
    environment:
      - RABBIT_URL=amqp://guest:guest@rabbitmq:5672/
      - WORKER_CONCURRENCY=10
    depends_on:
      rabbitmq:
        condition: service_healthy
    restart: unless-stopped
    deploy:
      replicas: 3 # 3 workers compiten por los mensajes de la misma cola
```

#### Aspectos Importantes

| Concepto          | Detalle                                                                              |
| ----------------- | ------------------------------------------------------------------------------------ |
| `durable=True`    | La cola persiste aunque RabbitMQ se reinicie                                         |
| `delivery_mode=2` | El mensaje se guarda en disco (no se pierde si el broker cae)                        |
| `prefetch_count`  | Limita cuántos mensajes toma un worker a la vez; distribuye la carga equitativamente |
| `basic_ack`       | Confirma que el mensaje fue procesado correctamente                                  |
| `basic_nack`      | Indica falla; el mensaje puede volver a la cola o ir a una DLQ                       |
| `replicas: 3`     | Tres workers compiten por mensajes — patrón _Competing Consumers_                    |

#### Cómo Ejecutar

```sh
# Levantar todo el stack (RabbitMQ + API + 3 workers)
docker compose up --build

# Comparar comportamiento síncrono vs asíncrono
curl "http://localhost:8000/sync?ms=500"    # bloquea 500ms
curl "http://localhost:8000/async?ms=500"   # retorna en microsegundos

# Ver colas y mensajes en el panel de RabbitMQ
# http://localhost:15672  (usuario: guest / contraseña: guest)
```

---

### Paso 5: Ejemplo Propio — Sistema de Pedidos E-commerce

#### Caso de Uso

Una plataforma de e-commerce recibe pedidos y los enruta a diferentes áreas de procesamiento según el tipo de envío elegido por el cliente:

- **Express** → entrega en 24 horas, requiere mensajero prioritario.
- **Normal** → entrega estándar de 3 a 5 días hábiles.
- **Internacional** → envío al exterior con trámites aduanales (15-30 días).

Cada tipo necesita un flujo de procesamiento diferente, por lo que se usa un **exchange de tipo topic** con routing keys categorizadas (`pedido.express`, `pedido.normal`, `pedido.internacional`).

#### ¿Por qué Topic Exchange?

| Exchange Type      | Enrutamiento                       | Cuándo usarlo                                           |
| ------------------ | ---------------------------------- | ------------------------------------------------------- |
| **Default** (`""`) | Routing key = nombre de cola       | Cola de trabajo simple (Pasos 1 y 2)                    |
| **Fanout**         | Todas las colas vinculadas         | Broadcast (Paso 3)                                      |
| **Direct**         | Coincidencia exacta de routing key | Un destino fijo por tipo de mensaje                     |
| **Topic**          | Patrones con `*` y `#`             | Múltiples categorías con reglas flexibles ← _este paso_ |

**Wildcards del topic exchange:**

| Wildcard | Significado             | Ejemplo                                                                            |
| -------- | ----------------------- | ---------------------------------------------------------------------------------- |
| `*`      | Exactamente una palabra | `pedido.*` captura `pedido.express` pero NO `pedido.express.bogota`                |
| `#`      | Cero o más palabras     | `pedido.#` captura `pedido.express`, `pedido.normal`, `pedido.normal.bogota`, etc. |

#### Arquitectura

```
ProductorPedidos
       │
       ├── "pedido.express"       ──> [cola_express]       ──> ConsumidorExpress
       ├── "pedido.normal"        ──> [cola_normal]        ──> ConsumidorNormal
       └── "pedido.internacional" ──> [cola_internacional] ──> ConsumidorInternacional
                    ▲
         [Exchange: pedidos_exchange (topic)]
```

#### Implementación en Java (`demo-PedidosEcommerce/`)

**`pom.xml`**

```xml
<dependency>
    <groupId>com.rabbitmq</groupId>
    <artifactId>amqp-client</artifactId>
    <version>5.21.0</version>
</dependency>
```

**`ProductorPedidos.java`**

```java
public class ProductorPedidos {
    private static final String EXCHANGE_NAME = "pedidos_exchange";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declarar exchange de tipo topic
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            // Enviar pedidos con diferentes routing keys
            enviarPedido(channel, "pedido.express",       "1001", "Laptop Gamer",        2, 3500000);
            enviarPedido(channel, "pedido.normal",        "1002", "Teclado Mecanico",    1,  250000);
            enviarPedido(channel, "pedido.internacional", "1003", "Camara DSLR",         1, 4200000);
            enviarPedido(channel, "pedido.express",       "1004", "Audifonos Bluetooth", 3,  180000);
            enviarPedido(channel, "pedido.normal",        "1005", "Mouse Gamer",         2,  120000);
        }
    }

    private static void enviarPedido(Channel channel, String routingKey,
                                     String id, String producto, int cantidad, long precio) throws Exception {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String mensaje = String.format(
            "{\"id\":\"%s\",\"producto\":\"%s\",\"cantidad\":%d,\"precio\":%d,\"timestamp\":\"%s\"}",
            id, producto, cantidad, precio, timestamp
        );
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, mensaje.getBytes("UTF-8"));
        System.out.printf("[Productor] Pedido enviado [%s] -> %s%n", routingKey, mensaje);
    }
}
```

**`ConsumidorExpress.java`**

```java
public class ConsumidorExpress {
    private static final String EXCHANGE_NAME = "pedidos_exchange";
    private static final String QUEUE_NAME    = "cola_express";
    private static final String ROUTING_KEY   = "pedido.express";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Mismo exchange que el productor
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // Declarar cola y vincularla con la routing key
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensaje = new String(delivery.getBody(), "UTF-8");
            System.out.println("[ConsumidorExpress] Pedido recibido: " + mensaje);
            // Simular procesamiento express
            System.out.println("  -> Verificando pago...");
            System.out.println("  -> Preparando envío en 24h...");
            System.out.println("  -> Asignando mensajero prioritario... HECHO.");
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        System.out.println("[ConsumidorExpress] Esperando pedidos express. CTRL+C para salir.");
    }
}
```

`ConsumidorNormal.java` y `ConsumidorInternacional.java` siguen la misma estructura, cambiando `QUEUE_NAME`, `ROUTING_KEY` y el mensaje de procesamiento.

#### Implementación en Python (`demoPy-PedidosEcommerce/`)

**`productor_pedidos.py`**

```python
import pika, json
from datetime import datetime

EXCHANGE_NAME = "pedidos_exchange"

def enviar_pedido(channel, routing_key, id_pedido, producto, cantidad, precio):
    pedido = {
        "id": id_pedido, "producto": producto,
        "cantidad": cantidad, "precio": precio,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=json.dumps(pedido).encode("utf-8")
    )
    print(f"[Productor] Pedido enviado [{routing_key}] -> {json.dumps(pedido)}")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    enviar_pedido(channel, "pedido.express",       "2001", "Laptop Gamer",        2, 3500000)
    enviar_pedido(channel, "pedido.normal",        "2002", "Teclado Mecanico",    1,  250000)
    enviar_pedido(channel, "pedido.internacional", "2003", "Camara DSLR",         1, 4200000)
    enviar_pedido(channel, "pedido.express",       "2004", "Audifonos Bluetooth", 3,  180000)
    enviar_pedido(channel, "pedido.normal",        "2005", "Mouse Gamer",         2,  120000)

    connection.close()

if __name__ == "__main__":
    main()
```

**`consumidor_express.py`**

```python
import pika, json

EXCHANGE_NAME = "pedidos_exchange"
QUEUE_NAME    = "cola_express"
ROUTING_KEY   = "pedido.express"

def callback(ch, method, properties, body):
    pedido = json.loads(body.decode("utf-8"))
    print(f"[ConsumidorExpress] Pedido recibido: {json.dumps(pedido)}")
    print("  -> Verificando pago...")
    print("  -> Preparando envío en 24h...")
    print("  -> Asignando mensajero prioritario... HECHO.\n")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")
    channel.queue_declare(queue=QUEUE_NAME)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    print("[ConsumidorExpress] Esperando pedidos express. CTRL+C para salir.")
    channel.start_consuming()

if __name__ == "__main__":
    main()
```

`consumidor_normal.py` y `consumidor_internacional.py` siguen la misma estructura con su propia `QUEUE_NAME` y `ROUTING_KEY`.

#### Formato del Mensaje

```json
{
  "id": "1001",
  "producto": "Laptop Gamer",
  "cantidad": 2,
  "precio": 3500000,
  "timestamp": "2026-05-23 14:30:00"
}
```

#### Salida Esperada

**Productor:**

```
[Productor] Pedido enviado [pedido.express] -> {"id":"1001","producto":"Laptop Gamer",...}
[Productor] Pedido enviado [pedido.normal] -> {"id":"1002","producto":"Teclado Mecanico",...}
[Productor] Pedido enviado [pedido.internacional] -> {"id":"1003","producto":"Camara DSLR",...}
[Productor] Todos los pedidos han sido enviados.
```

**ConsumidorExpress:**

```
[ConsumidorExpress] Pedido recibido: {"id":"1001","producto":"Laptop Gamer",...}
  -> Verificando pago...
  -> Preparando envío en 24h...
  -> Asignando mensajero prioritario... HECHO.
```

#### Cómo Ejecutar (Java)

```sh
# Compilar
mvn compile

# Terminal 1, 2 y 3 — consumidores
mvn exec:java -Dexec.mainClass="edu.unisabana.ConsumidorExpress"
mvn exec:java -Dexec.mainClass="edu.unisabana.ConsumidorNormal"
mvn exec:java -Dexec.mainClass="edu.unisabana.ConsumidorInternacional"

# Terminal 4 — productor
mvn exec:java -Dexec.mainClass="edu.unisabana.ProductorPedidos"
```

#### Cómo Ejecutar (Python)

```sh
pip install pika

# Terminal 1, 2 y 3 — consumidores
python consumidor_express.py
python consumidor_normal.py
python consumidor_internacional.py

# Terminal 4 — productor
python productor_pedidos.py
```

---

### Comparativa de Patrones Vistos en el Taller

| Módulo                      | Exchange Type  | Routing                      | Caso de Uso Real                                          |
| --------------------------- | -------------- | ---------------------------- | --------------------------------------------------------- |
| `demo` / `demoPy`           | Default (`""`) | Routing key = nombre de cola | Cola de tareas simple, procesamiento secuencial           |
| `demo-Exchange-Consumidor2` | Fanout         | Todas las colas vinculadas   | Logs, notificaciones a múltiples servicios                |
| `demo-API-Worker`           | Default (`""`) | Cola `jobs` única            | Procesamiento asíncrono con escalado horizontal           |
| `demo-PedidosEcommerce`     | Topic          | Patrón `pedido.*`            | Enrutamiento por categoría, microservicios especializados |

---

### Resumen y Conclusión

#### Resumen del Taller

- Aprendimos a utilizar RabbitMQ para la mensajería asincrónica en Java y Python.
- Implementamos los cuatro tipos de exchange principales: default, fanout, topic.
- Aplicamos el patrón Work Queue con múltiples workers en paralelo usando Docker.
- Creamos un ejemplo propio (sistema de pedidos e-commerce) que demuestra el enrutamiento inteligente con topic exchange.

#### Lo que Aprendimos

1. **Mensajería asincrónica:** los productores y consumidores no necesitan estar activos al mismo tiempo ni conocerse; RabbitMQ actúa como intermediario y almacena los mensajes hasta que sean consumidos.

2. **Tipos de exchange:** cada tipo resuelve un problema diferente de distribución de mensajes; elegir el correcto es clave para un diseño limpio.

3. **Patrones de arquitectura:**
   - **Simple Queue:** un productor, un consumidor, flujo básico.
   - **Pub-Sub / Fanout:** un evento que deben recibir múltiples servicios.
   - **Work Queue / Competing Consumers:** múltiples workers escalan el procesamiento horizontalmente.
   - **Topic Routing:** mensajes categorizados enrutados a consumidores especializados.

4. **Buenas prácticas (observadas en `demo-API-Worker`):**
   - Colas y mensajes durables para tolerar reinicios del broker.
   - `basic_ack` / `basic_nack` para garantía de entrega.
   - `prefetch_count` para distribución equitativa de carga entre workers.
   - Reintentos de conexión al arranque para sistemas resilientes.

#### Conclusión

RabbitMQ es una herramienta poderosa para construir sistemas distribuidos y escalables. Los conceptos explorados en este taller — colas, exchanges, routing keys y patrones de mensajería — son fundamentales en arquitecturas de microservicios y aplicaciones de alta disponibilidad. Continuar experimentando con casos de uso propios es la mejor forma de afianzar estos conocimientos.

### Ejecución del Taller

1. Configura tu entorno de desarrollo con las dependencias necesarias.
2. Implementa y ejecuta cada ejemplo de productor y consumidor en Java o Python.
3. Crea y comparte tu propio ejemplo utilizando los conceptos aprendidos.
4. Experimenta con otros casos de uso para profundizar en tu comprensión de RabbitMQ.
