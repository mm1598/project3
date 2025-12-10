import sys
import os
import struct
import csv

# --- Constants & Configuration ---
BLOCK_SIZE = 512
MAGIC_NUMBER = b'4348PRJ3'  # [cite: 75]
DEGREE = 10                 # Minimal degree t=10 [cite: 91]
MAX_KEYS = (2 * DEGREE) - 1 # 19 keys [cite: 91]
MAX_CHILDREN = 2 * DEGREE   # 20 children [cite: 91]

# Struct Formats (Big-endian >)
# Header: Magic(8s), RootID(Q), NextBlockID(Q), Unused(remaining) [cite: 74-78]
HEADER_FMT = f'>8sQQ{BLOCK_SIZE - 24}x'

# Node Header: BlockID(Q), ParentID(Q), NumKeys(Q) [cite: 94-96]
# Arrays: 19 Keys(Q), 19 Values(Q), 20 Children(Q) [cite: 97-99]
# Q = unsigned long long (8 bytes)
NODE_FMT = f'>QQQ{MAX_KEYS}Q{MAX_KEYS}Q{MAX_CHILDREN}Q'
NODE_HEADER_SIZE = 24 # 3 * 8 bytes
NODE_DATA_SIZE = (MAX_KEYS * 8) + (MAX_KEYS * 8) + (MAX_CHILDREN * 8) # 152 + 152 + 160
PADDING_SIZE = BLOCK_SIZE - (NODE_HEADER_SIZE + NODE_DATA_SIZE)

class BTreeNode:
    def __init__(self):
        self.block_id = 0
        self.parent_id = 0
        self.num_keys = 0
        self.keys = [0] * MAX_KEYS
        self.values = [0] * MAX_KEYS
        self.children = [0] * MAX_CHILDREN

    @property
    def is_leaf(self):
        # If the first child pointer is 0, it is a leaf [cite: 100]
        return self.children[0] == 0

    def serialize(self):
        """Converts node to binary block."""
        data = struct.pack(
            NODE_FMT,
            self.block_id,
            self.parent_id,
            self.num_keys,
            *self.keys,
            *self.values,
            *self.children
        )
        return data + (b'\x00' * PADDING_SIZE)

    @classmethod
    def deserialize(cls, data):
        """Parses binary block into Node object."""
        node = cls()
        unpacked = struct.unpack(NODE_FMT, data[:BLOCK_SIZE - PADDING_SIZE])
        
        node.block_id = unpacked[0]
        node.parent_id = unpacked[1]
        node.num_keys = unpacked[2]
        
        # Extract lists from the flattened tuple
        k_start = 3
        v_start = k_start + MAX_KEYS
        c_start = v_start + MAX_KEYS
        
        node.keys = list(unpacked[k_start:v_start])
        node.values = list(unpacked[v_start:c_start])
        node.children = list(unpacked[c_start:])
        
        return node

class IndexFile:
    def __init__(self, filename, mode='r+b'):
        self.filename = filename
        self.file = None
        self.root_id = 0
        self.next_block_id = 1
        
        if mode == 'create':
            if os.path.exists(filename):
                print(f"Error: File {filename} already exists.") # [cite: 17]
                sys.exit(1)
            self.file = open(filename, 'wb+')
            self._write_header() # Initialize header
        else:
            if not os.path.exists(filename):
                print(f"Error: File {filename} does not exist.") # [cite: 20]
                sys.exit(1)
            self.file = open(filename, 'r+b')
            self._read_header()

    def _write_header(self):
        """Writes the file header to Block 0[cite: 51]."""
        self.file.seek(0)
        data = struct.pack(HEADER_FMT, MAGIC_NUMBER, self.root_id, self.next_block_id)
        self.file.write(data)

    def _read_header(self):
        """Reads and validates the header."""
        self.file.seek(0)
        data = self.file.read(BLOCK_SIZE)
        if len(data) < BLOCK_SIZE:
             print("Error: Invalid index file (too small).")
             sys.exit(1)
             
        try:
            magic, root, next_id = struct.unpack(HEADER_FMT, data)
        except struct.error:
             print("Error: Invalid header format.")
             sys.exit(1)

        if magic != MAGIC_NUMBER:
            print("Error: Invalid magic number. Not a valid index file.") # [cite: 20]
            sys.exit(1)
            
        self.root_id = root
        self.next_block_id = next_id

    def read_node(self, block_id):
        """Reads a node from disk."""
        if block_id == 0: return None
        self.file.seek(block_id * BLOCK_SIZE)
        data = self.file.read(BLOCK_SIZE)
        return BTreeNode.deserialize(data)

    def write_node(self, node):
        """Writes a node to disk."""
        self.file.seek(node.block_id * BLOCK_SIZE)
        self.file.write(node.serialize())

    def allocate_node(self):
        """Allocates a new block ID and updates header."""
        new_id = self.next_block_id
        self.next_block_id += 1
        self._write_header() # Sync header [cite: 73]
        
        node = BTreeNode()
        node.block_id = new_id
        return node

    def close(self):
        self.file.close()

    # --- B-Tree Operations ---

    def search(self, key):
        """Searches for a key starting from root."""
        if self.root_id == 0:
            return None
        
        current = self.read_node(self.root_id)
        
        while True:
            i = 0
            # Find the first key greater than or equal to k
            while i < current.num_keys and key > current.keys[i]:
                i += 1
            
            # If found equal
            if i < current.num_keys and key == current.keys[i]:
                return (current.keys[i], current.values[i])
            
            # If leaf, not found
            if current.is_leaf:
                return None
            
            # Read child node (Constraints: Max 3 nodes in memory usually met here)
            child_id = current.children[i]
            if child_id == 0: return None
            current = self.read_node(child_id)

    def insert(self, key, value):
        """Inserts a key/value pair."""
        # Case 1: Tree is empty
        if self.root_id == 0:
            root = self.allocate_node()
            root.num_keys = 1
            root.keys[0] = key
            root.values[0] = value
            self.root_id = root.block_id
            self.write_node(root)
            self._write_header()
            return

        # Check for duplicate key (optional based on strict B-Tree, but good practice)
        if self.search(key) is not None:
             print(f"Error: Key {key} already exists.")
             return

        root = self.read_node(self.root_id)
        
        # Case 2: Root is full
        if root.num_keys == MAX_KEYS:
            new_root = self.allocate_node()
            new_root.children[0] = self.root_id
            
            # Old root becomes child of new root
            root.parent_id = new_root.block_id
            self.write_node(root)
            
            # Update header to point to new root
            self.root_id = new_root.block_id
            self._write_header()
            
            # Split the old root
            self.split_child(new_root, 0, root)
            
            # Insert into the new non-full root
            self.insert_non_full(new_root, key, value)
        else:
            self.insert_non_full(root, key, value)

    def split_child(self, parent, index, child):
        """Splits a full child node."""
        # Create new node z
        z = self.allocate_node()
        z.parent_id = parent.block_id
        
        # Median index for t=10 is 9. 
        # keys[0..8] (9 keys) stay in child. keys[9] goes up. keys[10..18] (9 keys) go to z.
        t = DEGREE
        split_idx = t - 1 # Index 9
        
        # z gets keys/values from t to 2t-1 (indices 10 to 18)
        # Number of items to move = 9
        num_items_to_move = DEGREE - 1
        
        z.num_keys = num_items_to_move
        
        # Copy keys/values to Z
        for j in range(num_items_to_move):
            z.keys[j] = child.keys[j + t]
            z.values[j] = child.values[j + t]
            # Reset old slots in child (cleanup)
            child.keys[j + t] = 0
            child.values[j + t] = 0

        # If not leaf, copy children to Z
        if not child.is_leaf:
            for j in range(DEGREE):
                z.children[j] = child.children[j + t]
                # Update parent pointer of moved children
                if z.children[j] != 0:
                    child_node = self.read_node(z.children[j])
                    child_node.parent_id = z.block_id
                    self.write_node(child_node)
                child.children[j + t] = 0

        child.num_keys = DEGREE - 1

        # Shift parent's children to make room for z
        for j in range(parent.num_keys, index, -1):
            parent.children[j + 1] = parent.children[j]
        
        parent.children[index + 1] = z.block_id

        # Shift parent's keys/values to make room for median
        for j in range(parent.num_keys - 1, index - 1, -1):
            parent.keys[j + 1] = parent.keys[j]
            parent.values[j + 1] = parent.values[j]

        # Move median key to parent
        parent.keys[index] = child.keys[split_idx]
        parent.values[index] = child.values[split_idx]
        parent.num_keys += 1
        
        # Cleanup child median slot
        child.keys[split_idx] = 0
        child.values[split_idx] = 0

        # Write all 3 nodes to disk (Parent, Child, Z) 
        self.write_node(child)
        self.write_node(z)
        self.write_node(parent)

    def insert_non_full(self, node, key, value):
        """Inserts into a non-full node."""
        i = node.num_keys - 1
        
        if node.is_leaf:
            # Shift keys/values to make room
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            
            node.keys[i + 1] = key
            node.values[i + 1] = value
            node.num_keys += 1
            self.write_node(node)
        else:
            # Find child to recurse into
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            
            child_block_id = node.children[i]
            child = self.read_node(child_block_id)
            
            if child.num_keys == MAX_KEYS:
                self.split_child(node, i, child)
                # After split, middle key goes to node. Determine which child to use.
                if key > node.keys[i]:
                    i += 1
                child = self.read_node(node.children[i])
            
            self.insert_non_full(child, key, value)

    def traverse(self, node_id, callback):
        """In-order traversal for print/extract."""
        if node_id == 0: return
        
        node = self.read_node(node_id)
        for i in range(node.num_keys):
            self.traverse(node.children[i], callback)
            callback(node.keys[i], node.values[i])
        
        self.traverse(node.children[node.num_keys], callback)

# --- CLI Handlers ---

def cmd_create(args):
    if len(args) != 2:
        print("Usage: project3 create <filename>")
        return
    idx = IndexFile(args[1], mode='create')
    idx.close()

def cmd_insert(args):
    if len(args) != 4:
        print("Usage: project3 insert <filename> <key> <value>")
        return
    
    idx = IndexFile(args[1])
    try:
        key = int(args[2])
        val = int(args[3])
        idx.insert(key, val) # [cite: 22]
    except ValueError:
        print("Error: Key and Value must be integers.")
    finally:
        idx.close()

def cmd_search(args):
    if len(args) != 3:
        print("Usage: project3 search <filename> <key>")
        return

    idx = IndexFile(args[1])
    try:
        key = int(args[2])
        result = idx.search(key) # [cite: 27]
        if result:
            print(f"{result[0]} {result[1]}")
        else:
            print("Error: Key not found.")
    except ValueError:
        print("Error: Key must be an integer.")
    finally:
        idx.close()

def cmd_load(args):
    if len(args) != 3:
        print("Usage: project3 load <filename> <csv_file>")
        return
        
    filename = args[1]
    csv_file = args[2]
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file {csv_file} does not exist.") # [cite: 32]
        return

    idx = IndexFile(filename)
    try:
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    k, v = int(row[0]), int(row[1])
                    idx.insert(k, v) # [cite: 34]
    except ValueError:
        print("Error: CSV must contain integers.")
    finally:
        idx.close()

def cmd_print(args):
    if len(args) != 2:
        print("Usage: project3 print <filename>")
        return

    idx = IndexFile(args[1])
    idx.traverse(idx.root_id, lambda k, v: print(f"{k} {v}")) # [cite: 43]
    idx.close()

def cmd_extract(args):
    if len(args) != 3:
        print("Usage: project3 extract <filename> <output_csv>")
        return

    idx_filename = args[1]
    out_filename = args[2]

    if os.path.exists(out_filename):
        print(f"Error: Output file {out_filename} already exists.") # [cite: 47]
        return

    idx = IndexFile(idx_filename)
    try:
        with open(out_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            # Use callback to write rows
            idx.traverse(idx.root_id, lambda k, v: writer.writerow([k, v])) # [cite: 48]
    finally:
        idx.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: project3 <command> [args...]")
        return

    # All commands should be lowercase [cite: 15]
    command = sys.argv[1].lower()
    
    commands = {
        'create': cmd_create,
        'insert': cmd_insert,
        'search': cmd_search,
        'load':   cmd_load,
        'print':  cmd_print,
        'extract': cmd_extract
    }

    if command in commands:
        commands[command](sys.argv)
    else:
        print(f"Error: Unknown command '{command}'")

if __name__ == "__main__":
    main()