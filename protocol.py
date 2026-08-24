def ping(sender, receiver):

    print(
        f"\nNode {sender.node_id} "
        f"-> PING -> "
        f"Node {receiver.node_id}"
    )

    response = pong(receiver, sender)

    return response


def pong(receiver, sender):

    print(
        f"Node {receiver.node_id} "
        f"-> PONG -> "
        f"Node {sender.node_id}"
    )

    return {
        "type": "PONG",
        "node_id": receiver.node_id
    }