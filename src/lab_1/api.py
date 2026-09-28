from fastapi import FastAPI

from lab_1.producer import publish

app = FastAPI()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/messages")
def send_message(message: str) -> dict:
    publish(message)
    return {"sent": message}
