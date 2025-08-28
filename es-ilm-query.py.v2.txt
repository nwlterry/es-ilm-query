import json
from elasticsearch import Elasticsearch
import getpass
from collections import defaultdict

# Prompt for Elasticsearch connection details
host = input("Enter Elasticsearch host (e.g., https://localhost:9200): ")
username = input("Enter username: ")
password = getpass.getpass("Enter password: ")

# Prompt for size threshold
size_threshold_str = input("Enter size threshold (e.g., 1gb, 500mb): ")

# Function to parse size string to bytes
def parse_size(size_str):
    size_str = size_str.lower().strip()
    if size_str.endswith('gb'):
        return float(size_str[:-2]) * 1024**3
    elif size_str.endswith('mb'):
        return float(size_str[:-2]) * 1024**2
    elif size_str.endswith('kb'):
        return float(size_str[:-2]) * 1024
    elif size_str.endswith('b'):
        return float(size_str[:-1])
    else:
        return float(size_str)  # assume bytes if no unit

size_threshold = parse_size(size_threshold_str)

# Connect to Elasticsearch, ignoring certificate verification
es = Elasticsearch(
    [host],
    basic_auth=(username, password),
    verify_certs=False
)

# Get all indices with relevant stats (using pri.store.size for primary size check)
indices = es.cat.indices(format="json", h="index,pri.store.size,pri,rep")

# Group indices by ILM policy
groups = defaultdict(list)

for idx in indices:
    pri_store_size_str = idx.get("pri.store.size", "0b")
    
    # Parse pri.store.size to bytes
    size_str = pri_store_size_str.lower().strip()
    if size_str.endswith(('b', 'k', 'm', 'g', 't', 'p')):
        unit = size_str[-1]
        value = float(size_str[:-1])
    else:
        unit = 'b'
        value = float(size_str)
    
    multipliers = {'b': 1, 'k': 1024, 'm': 1024**2, 'g': 1024**3, 't': 1024**4, 'p': 1024**5}
    size_bytes = value * multipliers.get(unit, 1)
    
    if size_bytes < size_threshold:
        index_name = idx["index"]
        
        # Check ILM info
        ilm_info = es.ilm.explain_index(index=index_name)
        index_ilm = ilm_info["indices"].get(index_name, {})
        
        if index_ilm.get("managed", False):
            policy = index_ilm["policy"]
            
            # Get shard counts
            pri_shards = int(idx.get("pri", "0"))
            rep_shards = int(idx.get("rep", "0"))
            total_shards = pri_shards * (1 + rep_shards)
            
            groups[policy].append({
                "index": index_name,
                "size_bytes": size_bytes,
                "total_shards": total_shards
            })

# Calculate stats per group
results = {}
for policy, idx_list in groups.items():
    num_indices = len(idx_list)
    total_shards = sum(i["total_shards"] for i in idx_list)
    results[policy] = {
        "num_indices": num_indices,
        "total_shards": total_shards,
        "indices": [i["index"] for i in idx_list]
    }

# Output results as JSON
print(json.dumps(results, indent=2))