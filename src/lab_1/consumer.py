import json

import httpx
import pika

from lab_1.producer import QUEUE_NAME, RABBITMQ_HOST, declare_topology, save_result

MAX_RETRIES = 3


def on_message(channel, method, properties, body: bytes) -> None:
    message = json.loads(body)
    request_id = message["id"]
    text = message["text"]

    retry_count = (properties.headers or {}).get("x-retry-count", 0)

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
        if retry_count < MAX_RETRIES - 1:
            print(
                f"AI call failed for {request_id} (attempt {retry_count + 1}/{MAX_RETRIES}): {e} — retrying"
            )
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=body,  # same payload, unchanged
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                    headers={"x-retry-count": retry_count + 1},
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print(
                f"AI call failed for {request_id} after {MAX_RETRIES} attempts: {e} — routing to DLQ"
            )
            save_result(
                request_id, {"id": request_id, "status": "error", "error": str(e)}
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main() -> None:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
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
