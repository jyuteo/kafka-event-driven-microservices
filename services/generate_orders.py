import os
import sys

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)

import time
import random

from streams.order_details import OrderDetailsStream


class OrderGenerator:
    """
    Generates dummy orders at regular time interval and pushes order details to kafka topic
    """

    ORDER_LIMIT = 100

    def __init__(self):
        self.order_details_stream = OrderDetailsStream()

    def generate(self):
        for i in range(self.ORDER_LIMIT):
            order_details = {
                "order_id": i,
                "user_id": "user_{}".format(i),
                "total_cost": random.randint(1, 10),
                "items": ",".join(
                    random.choices(
                        [
                            "burger",
                            "sandwich",
                            "noodles",
                            "sushi",
                        ],
                        k=2,
                    )
                ),
            }
            self.order_details_stream.add_order_details(order_details)
            print("Done sending order {}".format(i))
            time.sleep(2)


if __name__ == "__main__":
    service = OrderGenerator()
    service.generate()
