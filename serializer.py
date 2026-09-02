import socket
import cloudpickle
<<<<<<< HEAD
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
=======
>>>>>>> f93bb6ad56a86bfd382bda96753283e1de0c1433

def sample_task(a, b):
    return a + b

def serialize_task(func, args):
<<<<<<< HEAD
    return cloudpickle.dumps((func, args))

def receive_exact(sock, num_bytes):
    data = b""
    while len(data) < num_bytes:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed before all data was received.")
        data += chunk
    return data

def send_task(func, args, host="localhost", port=6000):
    payload = serialize_task(func, args)
    logging.info(f"Payload size: {len(payload)} bytes")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(len(payload).to_bytes(8, "big"))
            s.sendall(payload)
            result_len = int.from_bytes(receive_exact(s, 8), "big")
            result_data = receive_exact(s, result_len)
            return cloudpickle.loads(result_data)
    except ConnectionRefusedError:
        logging.error("Could not connect to executor. Is executor.py running?")
        return None
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return None

if __name__ == "__main__":
    response = send_task(sample_task, (5, 7))
    if response:
        if response["status"] == "success":
            logging.info(f"Task succeeded. Result: {response['result']}")
        else:
            logging.error(f"Task failed: {response['message']}")
=======
    """Turn a function + its arguments into bytes."""
    return cloudpickle.dumps((func, args))

def send_task(func, args, host="localhost", port=5000):
    payload = serialize_task(func, args)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(len(payload).to_bytes(8, "big"))
        s.sendall(payload)

        result_len = int.from_bytes(s.recv(8), "big")
        result_data = b""
        while len(result_data) < result_len:
            result_data += s.recv(4096)

        return cloudpickle.loads(result_data)

if __name__ == "__main__":
    result = send_task(sample_task, (5, 7))
    print("Result from executor:", result)
>>>>>>> f93bb6ad56a86bfd382bda96753283e1de0c1433
