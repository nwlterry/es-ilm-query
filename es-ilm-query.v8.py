import json
from elasticsearch import Elasticsearch
import getpass
from collections import defaultdict
import urllib3
import warnings
import base64

# Suppress all urllib3 warnings (including TLS-related)
urllib3.disable_warnings()

# Suppress warnings from Elasticsearch client
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Prompt for Elasticsearch connection details
host = input("Enter Elasticsearch host (e.g., https://localhost:9200): ")
username = input("Enter username: ")
password = getpass.getpass("Enter password: ")

# Prompt for size threshold
size_threshold_str = input("Enter size threshold (e.g., 1gb, 500mb): ")

# Prompt for output file
output_file = input("Enter output file path (e.g., output.json): ")

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
        try:
            return float(size_str)  # assume bytes if no unit
        except ValueError:
            print(f"Warning: Invalid size threshold '{size_str}', assuming 0 bytes")
            return 0.0

# Function to format bytes to human-readable string
def format_size(bytes):
    for unit, divisor in [('GB', 1024**3), ('MB', 1024**2), ('KB', 1024), ('B', 1)]:
        if bytes >= divisor:
            return f"{bytes / divisor:.2f}{unit}"
    return f"{bytes:.2f}B"

size_threshold = parse_size(size_threshold_str)

# Encode credentials for Basic Auth header
auth_string = f"{username}:{password}"
auth_encoded = base64.b64encode(auth_string.encode()).decode()
auth_header = {"Authorization": f"Basic {auth_encoded}"}

# Connect to Elasticsearch, ignoring certificate verification
es = Elasticsearch(
    [host],
    basic_auth=(username, password),
    verify_certs=False,
    ssl_show_warn=False
)

# Test authentication with a simple request
try:
    es_info = es.info()
    print("Successfully connected to Elasticsearch cluster")
except Exception as e:
    print(f"Error connecting to Elasticsearch: {str(e)}")
    indices = []
else:
    # Get all indices with relevant stats (using pri.store.size for primary size check)
    try:
        indices = es.cat.indices(format="json", h="index,pri.store.size,pri,rep")
    except Exception as e:
        print(f"Error fetching indices: {str(e)}")
        indices = []

# Group indices by ILM policy
groups = defaultdict(list)

for idx in indices:
    pri_store_size_str = idx.get("pri.store.size", "0b")
    
    # Parse pri.store.size to bytes
    size_str = pri_store_size_str.lower().strip()
    numeric_part = ''.join(c for c in size_str if c.isdigit() or c == '.')
    unit = size_str[len(numeric_part):] if numeric_part else 'b'
    
    try:
        value = float(numeric_part) if numeric_part else 0.0
    except ValueError:
        print(f"Warning: Could not parse size '{pri_store_size_str}' for index '{idx['index']}', assuming 0 bytes")
        value = 0.0
    
    # Convert to bytes
    multipliers = {'b': 1, 'kb': 1024, 'mb': 1024**2, 'gb': 1024**3, 'tb': 1024**4, 'pb': 1024**5}
    size_bytes = value * multipliers.get(unit, 1)
    
    if size_bytes < size_threshold:
        index_name = idx["index"]
        
        # Check ILM info using transport.perform_request with auth headers
        try:
            ilm_info = es.transport.perform_request("GET", f"/{index_name}/_ilm/explain", headers=auth_header)
            if isinstance(ilm_info, tuple):
                print(f"Warning: Unexpected tuple response for index '{index_name}': {ilm_info}")
                ilm_info = ilm_info[-1] if ilm_info else {}  # Take last element (likely body)
            if not isinstance(ilm_info, dict):
                print(f"Warning: Unexpected ILM response type for index '{index_name}': {type(ilm_info)}")
                ilm_info = {}
            index_ilm = ilm_info.get("indices", {}).get(index_name, {})
        except Exception as e:
            print(f"Warning: Failed to get ILM info for index '{index_name}': {str(e)}")
            index_ilm = {}
        
        if index_ilm.get("managed", False):
            policy = index_ilm["policy"]
            
            # Get shard counts
            pri_shards = int(idx.get("pri", "0"))
            rep_shards = int(idx.get("rep", "0"))
            total_shards = pri_shards * (1 + rep_shards)
            
            groups[policy].append({
                "index": index_name,
                "size_bytes": size_bytes,
                "size_readable": format_size(size_bytes),
                "total_shards": total_shards
            })

# Calculate stats per group
results = {}
for policy, idx_list in groups.items():
    num_indices = len(idx_list)
    total_shards = sum(i["total_shards"] for i in idx_list)
    total_size_bytes = sum(i["size_bytes"] for i in idx_list)
    results[policy] = {
        "num_indices": num_indices,
        "total_shards": total_shards,
        "total_size": format_size(total_size_bytes),
        "total_size_bytes": total_size_bytes,
        "indices": [
            {"name": i["index"], "size": i["size_readable"], "shards": i["total_shards"]}
            for i in idx_list
        ]
    }

# Output results to console
print(json.dumps(results, indent=2))

# Output results to file
try:
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Results written to {output_file}")
except Exception as e:
    print(f"Error writing to file '{output_file}': {str(e)}")
