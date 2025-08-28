import json
from elasticsearch import Elasticsearch
import getpass
from collections import defaultdict

def parse_size(size_str):
    if not size_str or size_str == '0':
        return 0.0
    size_str = size_str.lower().strip()
    units = {'b': 1, 'kb': 1024, 'mb': 1024**2, 'gb': 1024**3, 'tb': 1024**4}
    for unit, multiplier in units.items():
        if size_str.endswith(unit):
            try:
                return float(size_str[:-len(unit)]) * multiplier
            except ValueError:
                return 0.0
    try:
        return float(size_str)  # Assume bytes if no unit
    except ValueError:
        return 0.0

# Prompt for Elasticsearch connection details
host = input("Enter Elasticsearch host (e.g., http://localhost:9200): ").strip()
username = input("Enter username (leave blank if no authentication): ").strip()

if username:
    password = getpass.getpass("Enter password: ")
    es = Elasticsearch(hosts=[host], basic_auth=(username, password))
else:
    es = Elasticsearch(hosts=[host])

# Prompt for size threshold
threshold_str = input("Enter size threshold (e.g., 1gb, 500mb): ").strip()
size_threshold = parse_size(threshold_str)

# Get all indices with relevant stats
indices = es.cat.indices(format="json", h="index,store.size,pri.store.size,pri,rep")

# Group by ILM policy
groups = defaultdict(lambda: {'indices': [], 'num_indices': 0, 'total_shards': 0})

for index_info in indices:
    pri_store_size = index_info.get("pri.store.size", "0b")
    size_bytes = parse_size(pri_store_size)
    
    if size_bytes < size_threshold:
        index_name = index_info["index"]
        ilm_info = es.ilm.explain_index(index=index_name)
        index_ilm = ilm_info["indices"].get(index_name, {})
        
        if index_ilm.get("managed", False) and index_ilm.get("policy"):
            policy = index_ilm["policy"]
            groups[policy]['indices'].append(index_name)
            groups[policy]['num_indices'] += 1
            
            # Calculate total shards (primary * (replicas + 1))
            pri_shards = int(index_info.get('pri', 0))
            reps = int(index_info.get('rep', 0))
            total_shards = pri_shards * (reps + 1)
            groups[policy]['total_shards'] += total_shards

# Prepare results (exclude indices list if not needed in output)
result = {
    policy: {
        'num_indices': data['num_indices'],
        'total_shards': data['total_shards']
    } for policy, data in groups.items()
}

# Output results
print(json.dumps(result, indent=2))