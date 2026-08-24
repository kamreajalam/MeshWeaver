from bucket import KBucket


class RoutingTable:

    def __init__(self, k=3):
        self.bucket = KBucket(k)

    def add_peer(self, peer):
        self.bucket.add_peer(peer)

    def find_closest(self, target_id):
        return self.bucket.find_closest(target_id)