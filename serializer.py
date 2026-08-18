import socket
import cloudpickle

def sample_task(a, b):
    return a + b

def serialize_task(func, args):
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