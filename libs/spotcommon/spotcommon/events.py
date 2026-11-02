from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Protocol


class EventBus(Protocol):
    async def publish(self, topic: str, key: str, payload: dict) -> None: ...


class InMemoryBus:
    """Test double. `log` holds (topic, key, payload) in publish order."""

    def __init__(self) -> None:
        self.log: list[tuple[str, str, dict]] = []

    async def publish(self, topic: str, key: str, payload: dict) -> None:
        self.log.append((topic, key, payload))

    def topics(self) -> list[str]:
        return [t for t, _, _ in self.log]


class KafkaBus:
    """Idempotent, acks=all producer. Key = gpu_type, so one market == one partition == total order."""

    def __init__(self, bootstrap: str) -> None:
        self._bootstrap = bootstrap
        self._producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap, acks="all", enable_idempotence=True,
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
            key_serializer=lambda k: k.encode(),
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()

    async def publish(self, topic: str, key: str, payload: dict) -> None:
        if self._producer is None:
            await self.start()
        await self._producer.send_and_wait(topic, payload, key=key)


async def consume(bootstrap: str, group: str, topics: list[str]) -> AsyncIterator[tuple[str, dict, object]]:
    """Yield (topic, payload, commit) — call `await commit()` only after the handler's DB txn commits."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        *topics, bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False,
        auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode()),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            async def commit(_msg=msg):
                await consumer.commit()
            yield msg.topic, msg.value, commit
    finally:
        await consumer.stop()
