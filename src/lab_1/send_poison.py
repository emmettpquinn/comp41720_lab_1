import uuid

from lab_1.producer import publish

publish({"id": str(uuid.uuid4()), "text": "__force_dlq_test__"})
print("Poison message sent.")