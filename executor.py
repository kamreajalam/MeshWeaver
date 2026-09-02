import logging
import socket

import cloudpickle


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def deserialize_task(payload):
    """Turn bytes back into a function and its arguments."""
    return cloudpickle.loads(payload)


def run_task(func, args):
    """Run a task and return a success or error response."""
    try:
        result = func(*args)
        return {"status": "success", "result": result}

    except Exception as error:
        logging.error("Task execution failed: %s", error)
        return {"status": "error", "message": str(error)}


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


def start_executor(host="localhost", port=6000):
    """Listen for incoming tasks and return their results."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(1)

        logging.info("Executor listening on %s:%s...", host, port)

        while True:
            conn, addr = server.accept()

            with conn:
                logging.info("Connected by %s", addr)

                try:
                    payload_len = int.from_bytes(
                        receive_data(conn, 8),
                        "big",
                    )
                    payload = receive_data(conn, payload_len)

                    func, args = deserialize_task(payload)
                    logging.info(
                        "Running task: %s with args %s",
                        func.__name__,
                        args,
                    )

                    result = run_task(func, args)
                    result_data = cloudpickle.dumps(result)

                    conn.sendall(
                        len(result_data).to_bytes(8, "big")
                    )
                    conn.sendall(result_data)

                    logging.info("Task complete: %s", result)

                except Exception as error:
                    logging.error(
                        "Error handling connection: %s",
                        error,
                    )


if __name__ == "__main__":
    start_executor()