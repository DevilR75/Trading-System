# Main script for the Exchange Application using RabbitMQ – manages order matching and trade execution
import argparse
import json
import pika

# Constants for connection and RabbitMQ object names
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
EXCHANGE_NAME = "orders_exchange"
ORDERS_QUEUE = "orders"
TRADES_QUEUE = "trades"

class Exchange:
    def __init__(self):
        """
        order_books — a dictionary where each key is a symbol (string) and the value is a dictionary:
            { 'buy': [list of buy orders], 'sell': [list of sell orders] }
        Each order is stored as a dictionary:
            { 'username': str, 'side': 'BUY'/'SELL', 'symbol': str, 'price': float, 'quantity': int }
        """
        self.order_books = {}  # Stores order books for each symbol

    def _init_symbol_if_needed(self, symbol):
        """
        Helper method that creates empty lists for buy and sell orders if the symbol is new.
        This ensures that there is always an order book available for any given symbol.
        """
        if symbol not in self.order_books:
            self.order_books[symbol] = {'buy': [], 'sell': []}

    def on_order_received(self, order):
        """
        Processes a new incoming order.
        
        Steps:
          1. Ensure the order book exists for the symbol.
          2. Depending on the side (BUY/SELL), determine the opposite and same-side order lists.
          3. Attempt to match the incoming order against the opposite orders:
             - For BUY orders: match with sell orders that have a price <= buy price.
             - For SELL orders: match with buy orders that have a price >= sell price.
          4. Loop through the sorted list of opposite orders to find a match.
             - This loop supports partial fills: the incoming order can be partially matched against one or more opposite orders.
          5. For each match found:
             - Determine the trade quantity based on the available quantities.
             - Adjust or remove the matched opposite order accordingly.
             - Record the trade with the price taken from the existing order in the book.
          6. If after matching there is any remaining quantity, add it to the appropriate side of the order book.
          
        Returns:
            A list of trade dictionaries for the executed matches.
        """
        symbol = order["symbol"]
        side = order["side"]
        username = order["username"]
        price = order["price"]
        quantity = order["quantity"]

        # Ensure that an order book exists for the given symbol.
        self._init_symbol_if_needed(symbol)
        trades = []  # List to store any generated trades

        # Determine which lists to use based on order side:
        # For a BUY order, match against sell orders; for a SELL order, match against buy orders.
        if side == "BUY":
            opposite_list = self.order_books[symbol]['sell']
            same_side_list = self.order_books[symbol]['buy']
            # Matching condition: sell order price must be less than or equal to buy order price.
            def match_condition(opposite_price, my_price):
                return opposite_price <= my_price
            # Sort sell orders by price in ascending order to get the best (lowest) price first.
            sort_opposite = lambda orders: sorted(orders, key=lambda x: x["price"])
        else:  # SELL order
            opposite_list = self.order_books[symbol]['buy']
            same_side_list = self.order_books[symbol]['sell']
            # Matching condition: buy order price must be greater than or equal to sell order price.
            def match_condition(opposite_price, my_price):
                return opposite_price >= my_price
            # Sort buy orders by price in descending order to get the best (highest) price first.
            sort_opposite = lambda orders: sorted(orders, key=lambda x: x["price"], reverse=True)

        remaining_qty = quantity  # Quantity of the incoming order that still needs to be matched
        matched = True  # Flag to indicate if a match was found in the last iteration

        # Attempt to sequentially match the incoming order with orders from the opposite side.
        # This loop allows for partial fills if one opposite order cannot completely satisfy the incoming order.
        while remaining_qty > 0 and matched and len(opposite_list) > 0:
            # Sort the opposite orders to ensure we check the best available price first.
            opposite_list_sorted = sort_opposite(opposite_list)
            matched = False  # Reset flag; if no matching order is found in this iteration, the loop will exit.

            # Iterate over the sorted opposite orders to find a matching order.
            for opp_order in opposite_list_sorted:
                if match_condition(opp_order["price"], price):
                    # Matching order found – process the trade.
                    matched = True

                    # Find the index of this order in the original unsorted list.
                    original_index = opposite_list.index(opp_order)
                    opp_qty = opp_order["quantity"]

                    # Determine the trade quantity based on the available quantity of the opposite order.
                    if opp_qty == remaining_qty:
                        # Exact match: both orders are completely filled.
                        trade_qty = remaining_qty
                        opposite_list.pop(original_index)  # Remove the opposite order from the order book.
                        remaining_qty = 0
                    elif opp_qty > remaining_qty:
                        # The opposite order can completely fill the incoming order.
                        trade_qty = remaining_qty
                        opp_order["quantity"] = opp_qty - remaining_qty  # Reduce the quantity of the opposite order.
                        remaining_qty = 0
                    else:  # opp_qty < remaining_qty
                        # The opposite order is completely filled, but the incoming order still has remaining quantity.
                        trade_qty = opp_qty
                        opposite_list.pop(original_index)  # Remove the fully executed opposite order.
                        remaining_qty -= opp_qty

                    # Trade price is typically set to the price of the order that was already in the book.
                    trade_price = opp_order["price"]

                    # Record the trade information.
                    trade_info = {
                        "symbol": symbol,
                        "buyer": username if side == "BUY" else opp_order["username"],
                        "seller": opp_order["username"] if side == "BUY" else username,
                        "price": trade_price,
                        "quantity": trade_qty
                    }
                    trades.append(trade_info)
                    # Break out of the loop to re-check for additional matches with the updated remaining quantity.
                    break

        # If the incoming order is not fully matched, add the remaining portion to the order book.
        if remaining_qty > 0:
            # Build the remaining order structure.
            new_order = {
                "username": username,
                "symbol": symbol,
                "side": side,
                "price": price,
                "quantity": remaining_qty
            }
            same_side_list.append(new_order)
            # Sort the order book to maintain the best price ordering:
            # For BUY orders: descending order; for SELL orders: ascending order.
            if side == "BUY":
                same_side_list.sort(key=lambda x: x["price"], reverse=True)
            else:
                same_side_list.sort(key=lambda x: x["price"])

        # Return all trades that were executed during this matching process.
        return trades

def main():
    # Parse command-line arguments for RabbitMQ host and port.
    parser = argparse.ArgumentParser(
        description="Exchange Application (multi-symbol, partial fill)."
    )
    parser.add_argument("--host", "-H", default=RABBITMQ_HOST, help="RabbitMQ host (default=localhost)")
    parser.add_argument("--port", "-P", type=int, default=RABBITMQ_PORT, help="RabbitMQ port (default=5672)")
    args = parser.parse_args()

    # Instantiate the Exchange class which manages the order book.
    exchange_instance = Exchange()

    # Set up the connection to RabbitMQ.
    connection_params = pika.ConnectionParameters(host=args.host, port=args.port)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    # Declare a direct exchange for routing orders.
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")
    print(f"✅ Exchange '{EXCHANGE_NAME}' is ready.")

    # Declare the orders queue and bind it to the exchange using the queue name as the routing key.
    channel.queue_declare(queue=ORDERS_QUEUE)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=ORDERS_QUEUE, routing_key=ORDERS_QUEUE)

    # Declare the trades queue which will receive executed trade messages.
    channel.queue_declare(queue=TRADES_QUEUE)

    def callback(ch, method, properties, body):
        """
        Callback function that is triggered for each incoming order message.
        It decodes the order, processes it using the Exchange instance, publishes any resulting trades,
        and acknowledges the message.
        """
        try:
            order = json.loads(body.decode("utf-8"))
        except Exception as e:
            print("Error decoding order:", e)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[x] Received order: {order}")
        trades = exchange_instance.on_order_received(order)
        if trades:
            # For each executed trade, publish it to the trades queue.
            for t in trades:
                channel.basic_publish(
                    exchange="",  # Use the default exchange for publishing trades.
                    routing_key=TRADES_QUEUE,
                    body=json.dumps(t).encode("utf-8")
                )
                print(f"[x] Trade executed: {t}")
        else:
            print("[ ] No trade executed. New/remaining order added to order book.")

        # Acknowledge the message to remove it from the queue.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming messages from the orders queue.
    channel.basic_consume(queue=ORDERS_QUEUE, on_message_callback=callback, auto_ack=False)

    print("[*] Exchange started (multi-symbol, partial fill). Waiting for orders...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Shutting down exchange...")
    finally:
        channel.stop_consuming()
        connection.close()

if __name__ == "__main__":
    main()
