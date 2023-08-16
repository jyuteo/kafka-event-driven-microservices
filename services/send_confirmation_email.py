import os
import sys

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)

import json

from streams.confirmed_order import ConfirmedOrderStream


class SendConfirmationEmail:
    """
    Consumes confirmed_order topic, and sends confirmation email for every confirmed order
    """

    def __init__(self):
        self.confirmed_order_stream = ConfirmedOrderStream()

    def listen_and_send_confirmation(self):
        while True:
            consumed_message = self.confirmed_order_stream.get_confirmed_order()
            confirmed_order = json.loads(consumed_message.value.decode())
            print(
                "Received confirmed order. Sending email to {}".format(
                    confirmed_order["customer_email"]
                )
            )


if __name__ == "__main__":
    service = SendConfirmationEmail()
    service.listen_and_send_confirmation()
