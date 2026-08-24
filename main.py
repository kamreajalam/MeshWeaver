from node import Node
from distance import xor_distance
from protocol import ping
# creating a node useing loops
nodes = []
for i in range(1, 11):
    node = Node(i * 10)
    nodes.append(node)
print("--- NODES ---")
for node in nodes:
    print(node)

# add node into peers useing loop
node_a = nodes[0]
for peer in nodes[1:]:
    node_a.routing_table.add_peer(peer)

# To node ping and pong 
print("\n--- PING / PONG ---")
for peer in nodes[1:]:
    response = ping(node_a, peer)
    print("Response:", response)

# calculate node distances
print("\n--- XOR DISTANCE ---")
for peer in nodes[1:]:
    distance = xor_distance(
        node_a.node_id,
        peer.node_id
    )
    print(
        f"Distance between Node "
        f"{node_a.node_id} and Node "
        f"{peer.node_id} = {distance}"
    )

#finding nodes use targets value
print("\n--- FIND_NODE ---")
for target_node in nodes[1:]:
    target_id = target_node.node_id
    print(f"\nTarget ID: {target_id}")
    closest = node_a.find_node(target_id)
    print("Closest nodes:")
for node in closest:

    distance = xor_distance(
        node.node_id,
        target_id
    )

    print(
        f"Node ID: {node.node_id}, "
        f"XOR distance: {distance}"
    )
    