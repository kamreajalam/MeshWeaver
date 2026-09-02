import logging
import socket

import cloudpickle


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def sample_task(a, b):
    return a + b


def serialize_task(func, args):
    """Turn a function and its arguments into bytes."""
    return cloudpickle.dumps((func, args))


def receive_data(sock, num_bytes):
    """Read exactly num_bytes from a socket."""
    data = b""

    while len(data) < num_bytes:
        remaining = num_bytes - len(data)
        chunk = sock.recv(min(4096, remaining))

        if not chunk:
            raise ConnectionError(
                "Connection closed before all data was received."
            )

        data += chunk

    return data


def send_task(func, args, host="localhost", port=6000):
    """Send a task to the executor and return its response."""
    payload = serialize_task(func, args)
    logging.info("Payload size: %s bytes", len(payload))

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(len(payload).to_bytes(8, "big"))
            sock.sendall(payload)

            result_len = int.from_bytes(receive_data(sock, 8), "big")
            result_data = receive_data(sock, result_len)

            return cloudpickle.loads(result_data)

    except ConnectionRefusedError:
        logging.error("Could not connect to executor. Is executor.py running?")
        return None

    except Exception as error:
        logging.error("Unexpected error: %s", error)
        return None


if __name__ == "__main__":
    response = send_task(sample_task, (5, 7))

    if response:
        if response["status"] == "success":
            logging.info("Task succeeded. Result: %s", response["result"])
        else:
            logging.error("Task failed: %s", response["message"])