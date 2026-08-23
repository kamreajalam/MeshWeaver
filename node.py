from routing_table import RoutingTable


class Node:

    def __init__(self, node_id):
        self.node_id = node_id
        self.routing_table = RoutingTable(k=3)

    def __repr__(self):
        return f"Node({self.node_id})"

    def find_node(self, target_id):

        print(
            f"\nNode {self.node_id} "
            f"searching for Node ID {target_id}"
        )

        return self.routing_table.find_closest(target_id)
