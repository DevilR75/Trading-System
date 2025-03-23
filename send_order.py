# This script injects orders into the system via RabbitMQ
import json
import pika

def main():
    while True:
        print("=== Send Order via Keyboard Input ===")

        # Prompt the user for each required field
        username = input("Enter username: ")
        
        # Validate ticker symbol: must be all uppercase letters
        while True:
            symbol = input("Enter financial ticker symbol (e.g. 'XYZ'): ")
            if symbol.isupper():
                break
            else:
                print("Invalid symbol. Please enter the symbol using all uppercase letters.")

        # Validate side (BUY or SELL)
        while True:
            side = input("Enter side (BUY or SELL): ").upper()
            if side in ["BUY", "SELL"]:
                break
            else:
                print("Invalid side. Please enter 'BUY' or 'SELL'.")

        # Quantity (optional, default=100)
        quantity_str = input("Enter quantity [default=100]: ")
        if quantity_str.strip() == "":
            quantity = 100
        else:
            quantity = int(quantity_str)

        # Price (float, required)
        while True:
            price_str = input("Enter price: ")
            try:
                price = float(price_str)
                break
            except ValueError:
                print("Invalid price. Please enter a numeric value.")

        # Host (optional, default=localhost)
        host_str = input("Enter RabbitMQ host [default=localhost]: ")
        host = host_str.strip() if host_str.strip() else "localhost"

        # Port (optional, default=5672)
        port_str = input("Enter RabbitMQ port [default=5672]: ")
        if port_str.strip():
            port = int(port_str.strip())
        else:
            port = 5672

        # Construct the order message
        order_msg = {
            "username": username,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price
        }

        # Connect to RabbitMQ
        connection_params = pika.ConnectionParameters(host=host, port=port)
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Declare the queue (orders)
        channel.queue_declare(queue="orders")

        # Publish the message
        channel.basic_publish(
            exchange="",
            routing_key="orders",
            body=json.dumps(order_msg).encode("utf-8")
        )

        print(f"\n[x] Order sent: {order_msg}")

        # Close the connection
        connection.close()

        # Ask the user if they want to send another order
        again = input("\nSend another order? (y/n): ")
        if again.lower() != 'y':
            break

if __name__ == "__main__":
    main()