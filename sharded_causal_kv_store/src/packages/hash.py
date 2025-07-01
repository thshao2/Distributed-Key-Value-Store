import hashlib
import bisect

class HashCircle:
    def __init__(self, virtual_nodes: int = 100):
        """
        Initialize an empty consistent hash circle.
        
        Args:
            virtual_nodes (int): Number of virtual nodes to create per physical shard.
        """
        # The circle is a sorted list of tuples: (hash_position, shard_name)
        self.circle = []
        self.virtual_nodes = virtual_nodes

    def _hash(self, item: str) -> int:
        """Compute an integer hash for a given string using MD5."""
        md5_digest = hashlib.md5(item.encode("utf-8")).hexdigest()
        return int(md5_digest, 16)

    def add_shard(self, shard_name: str):
        """
        Add a physical shard to the circle by creating virtual nodes.
        
        Each virtual node is represented by the string "{shard_name}#{i}".
        """
        for i in range(self.virtual_nodes):
            vnode_key = f"{shard_name}#{i}"
            position = self._hash(vnode_key)
            # Insert the tuple (position, shard_name) into the sorted circle.
            bisect.insort(self.circle, (position, shard_name))

    def remove_shard(self, shard_name: str):
        """
        Remove a physical shard (and all its virtual nodes) from the circle.
        """
        self.circle = [(pos, s) for pos, s in self.circle if s != shard_name]

    def get_shard_for_key(self, key: str) -> str:
        """
        Given a key, hash it and find the physical shard that should store it.
        
        The key is assigned to the shard corresponding to the virtual node with the
        smallest hash value greater than or equal to the key's hash (wrapping around if necessary).
        """
        key_hash = self._hash(key)
        index = bisect.bisect_right(self.circle, (key_hash, ""))
        if index == len(self.circle):
            index = 0  # Wrap around.
        return self.circle[index][1]

    def update_shards(self, shard_names: list[str]):
        """
        Replace the current circle with the given list of physical shard names.
        
        For each shard, create the configured number of virtual nodes.
        """
        new_circle = []
        for shard in shard_names:
            for i in range(self.virtual_nodes):
                vnode_key = f"{shard}#{i}"
                position = self._hash(vnode_key)
                new_circle.append((position, shard))
        self.circle = sorted(new_circle)

    def redistribute_keys(self, key_list: list[str]) -> dict:
        """
        Given a list of keys, return a dictionary mapping each physical shard
        to the list of keys that should be stored on that shard.
        
        Example output: {"Shard1": ["key1", "key5"], "Shard2": ["key2", "key3"], ...}
        """
        distribution = {}
        # Initialize dictionary for each shard present in the circle.
        for _, shard in self.circle:
            distribution.setdefault(shard, [])
        for key in key_list:
            shard = self.get_shard_for_key(key)
            distribution[shard].append(key)
        
        return distribution

    def __str__(self):
        return f"HashCircle({self.circle})"
