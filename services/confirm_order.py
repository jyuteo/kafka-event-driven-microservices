import os
import sys

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)

import json

from streams.order_details import OrderDetailsStream
from streams.confirmed_order import ConfirmedOrderStream


class ConfirmOrder:
    """
    Consumes order_details topic, and transforms it to confirmed order and pushes to confirmed_order topic
    """

    def __init__(self):
        self.order_details_stream = OrderDetailsStream()
        self.confirmed_order_stream = ConfirmedOrderStream()

    def listen_and_process_order(self):
        while True:
            consumed_message = self.order_details_stream.get_order_details()
            if consumed_message:
                order_details = json.loads(consumed_message.value.decode())
                confirmed_order = {
                    "customer_id": order_details["user_id"],
                    "customer_email": "{}@gmail.com".format(order_details["user_id"]),
                    "total_cost": order_details["total_cost"],
                }
                print(confirmed_order)
                self.confirmed_order_stream.add_confirmed_order(confirmed_order)


if __name__ == "__main__":
    service = ConfirmOrder()
    service.listen_and_process_order()
