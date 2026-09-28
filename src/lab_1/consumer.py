import pika

from lab_1.producer import QUEUE_NAME, RABBITMQ_HOST


def on_message(channel, method, properties, body: bytes) -> None:
    print(f"Received: {body.decode()}")
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
    print(f"Waiting for messages on '{QUEUE_NAME}'. Press Ctrl+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
