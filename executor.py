import socket
import cloudpickle

def deserialize_task(payload):
    """Turn bytes back into a function + its arguments."""
    return cloudpickle.loads(payload)

def run_task(func, args):
    try:
        return func(*args)
    except Exception as e:
        return f"Error executing function: {e}"

def start_executor(host="localhost", port=5000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen(1)
        print(f"Executor listening on {host}:{port}...")

        while True:
            conn, addr = s.accept()
            with conn:
                print("Connected by", addr)

                payload_len = int.from_bytes(conn.recv(8), "big")
                payload = b""
                while len(payload) < payload_len:
                    payload += conn.recv(4096)

                func, args = deserialize_task(payload)
                result = run_task(func, args)

                result_data = cloudpickle.dumps(result)
                conn.sendall(len(result_data).to_bytes(8, "big"))
                conn.sendall(result_data)

if __name__ == "__main__":
    start_executor()