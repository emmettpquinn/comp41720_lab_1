import pika

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "lab1"


def publish(message: str) -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=message.encode())
    finally:
        connection.close()
