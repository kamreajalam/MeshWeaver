import socket
import cloudpickle
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def deserialize_task(payload):
    """Turn bytes back into a function + its arguments."""
    return cloudpickle.loads(payload)

def run_task(func, args):
    try:
        result = func(*args)
        return {"status": "success", "result": result}
    except Exception as e:
        logging.error(f"Task execution failed: {e}")
        return {"status": "error", "message": str(e)}

def receive_exact(conn, num_bytes):
    data = b""
    while len(data) < num_bytes:
        chunk = conn.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed before all data was received.")
        data += chunk
    return data

def start_executor(host="localhost", port=6000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(1)
        logging.info(f"Executor listening on {host}:{port}...")
        while True:
            conn, addr = s.accept()
            with conn:
                logging.info(f"Connected by {addr}")
                try:
                    payload_len = int.from_bytes(receive_exact(conn, 8), "big")
                    payload = receive_exact(conn, payload_len)
                    func, args = deserialize_task(payload)
                    logging.info(f"Running task: {func.__name__} with args {args}")
                    result = run_task(func, args)
                    result_data = cloudpickle.dumps(result)
                    conn.sendall(len(result_data).to_bytes(8, "big"))
                    conn.sendall(result_data)
                    logging.info(f"Task complete: {result}")
                except Exception as e:
                    logging.error(f"Error handling connection: {e}")

if __name__ == "__main__":
    start_executor()