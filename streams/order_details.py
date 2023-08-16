import json

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from typing import Dict


class OrderDetailsStream:
    KAFKA_SERVER = "localhost:29092"
    KAFKA_ORDER_DETAILS_TOPIC = "order_details"

    def __init__(self):
        self.admin = KafkaAdminClient(bootstrap_servers=self.KAFKA_SERVER)
        self._delete_topic_if_exists()

        self.producer = KafkaProducer(bootstrap_servers=self.KAFKA_SERVER)
        self.consumer = KafkaConsumer(
            self.KAFKA_ORDER_DETAILS_TOPIC,
            bootstrap_servers=self.KAFKA_SERVER,
            auto_offset_reset="earliest",
        )

    def _delete_topic_if_exists(self):
        try:
            self.admin.delete_topics(topics=[self.KAFKA_ORDER_DETAILS_TOPIC])
        except Exception as e:
            print("unable to delete topic", e)

    def add_order_details(self, order_details: Dict):
        self.producer.send(
            self.KAFKA_ORDER_DETAILS_TOPIC,
            json.dumps(order_details).encode("utf-8"),
        )

    def get_order_details(self):
        return next(self.consumer)
