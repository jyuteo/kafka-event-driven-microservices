import json

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from typing import Dict


class ConfirmedOrderStream:
    KAFKA_SERVER = "localhost:29092"
    KAFKA_CONFIRMED_ORDER_TOPIC = "confirmed_order"

    def __init__(self):
        self.admin = KafkaAdminClient(bootstrap_servers=self.KAFKA_SERVER)
        self._delete_topic_if_exists()

        self.producer = KafkaProducer(bootstrap_servers=self.KAFKA_SERVER)
        self.consumer = KafkaConsumer(
            self.KAFKA_CONFIRMED_ORDER_TOPIC,
            bootstrap_servers=self.KAFKA_SERVER,
            auto_offset_reset="earliest",
        )

    def _delete_topic_if_exists(self):
        try:
            self.admin.delete_topics(topics=[self.KAFKA_CONFIRMED_ORDER_TOPIC])
        except Exception as e:
            print("unable to delete topic", e)

    def add_confirmed_order(self, order: Dict):
        self.producer.send(
            self.KAFKA_CONFIRMED_ORDER_TOPIC,
            json.dumps(order).encode("utf-8"),
        )

    def get_confirmed_order(self):
        return next(self.consumer)
