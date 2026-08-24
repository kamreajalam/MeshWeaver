from distance import xor_distance


class KBucket:

    def __init__(self, k=3):
        self.k = k
        self.peers = []

    def add_peer(self, peer):

        if peer in self.peers:
            return

        if len(self.peers) < self.k:
            self.peers.append(peer)
            print(f"Added {peer} to bucket")

        else:
            print("Bucket is full")

    def remove_peer(self, peer):

        if peer in self.peers:
            self.peers.remove(peer)
            print(f"Removed {peer}")

    def contains(self, peer):

        return peer in self.peers

    def find_closest(self, target_id):

        return sorted(
            self.peers,
            key=lambda peer: xor_distance(
                peer.node_id,
                target_id
            )
        )