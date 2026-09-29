import json

import httpx
import pika

from lab_1.producer import QUEUE_NAME, RABBITMQ_HOST, declare_topology, save_result


def on_message(channel, method, properties, body: bytes) -> None:
    message = json.loads(body)
    request_id = message["id"]
    text = message["text"]

    try:
        response = httpx.post(
            "http://localhost:11434/api/generate",
            json={"model": "llama3.2:1b", "prompt": text, "stream": False},
            timeout=60.0,
        )
        response.raise_for_status()
        ai_output = response.json()["response"]

        save_result(
            request_id, {"id": request_id, "status": "completed", "result": ai_output}
        )
        print(f"Completed {request_id}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except httpx.HTTPError as e:
        print(f"AI call failed for {request_id}: {e} — requeueing")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_consume(
        queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False
    )
    print(f"Waiting for messages on '{QUEUE_NAME}'. Press Ctrl+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
