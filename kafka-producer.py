import asyncio
from aiokafka import AIOKafkaProducer

async def produce():
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        await producer.send_and_wait("my_topic", b"Hello Kafka!")
    finally:
        await producer.stop()  # Ensure the producer is closed properly

if __name__ == "__main__":
    asyncio.run(produce())
