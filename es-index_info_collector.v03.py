import json
import pandas as pd
from elasticsearch import Elasticsearch
import argparse
from collections import defaultdict
import urllib3
import warnings
import base64
from datetime import datetime

# Suppress all urllib3 warnings (including TLS-related)
urllib3.disable_warnings()
# Suppress warnings from Elasticsearch client
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Elasticsearch Index Info Collector")
parser.add_argument("--host", required=True, help="Elasticsearch host (e.g., https://localhost:9200)")
parser.add_argument("--username", required=True, help="Elasticsearch username")
parser.add_argument("--password", required=True, help="Elasticsearch password")
args = parser.parse_args()

# Set output file names based on script name and current date
current_date = datetime.now().strftime("%Y-%m-%d")
script_name = "es-index_info_collector"
json_output_file = f"{script_name}_{current_date}.json"
csv_output_file = f"{script_name}_{current_date}.csv"

# Function to format bytes to human-readable string
def format_size(bytes):
    for unit, divisor in [('GB', 1024**3), ('MB', 1024**2), ('KB', 1024), ('B', 1)]:
        if bytes >= divisor:
            return f"{bytes / divisor:.2f}{unit}"
    return f"{bytes:.2f}B"

# Encode credentials for Basic Auth header
auth_string = f"{args.username}:{args.password}"
auth_encoded = base64.b64encode(auth_string.encode()).decode()
auth_header = {"Authorization": f"Basic {auth_encoded}"}

# Connect to Elasticsearch, ignoring certificate verification
es = Elasticsearch(
    [args.host],
    basic_auth=(args.username, args.password),
    verify_certs=False,
    ssl_show_warn=False
)

# Test authentication with a simple request
try:
    es_info = es.info()
    print("Successfully connected to Elasticsearch cluster")
    print(f"Elasticsearch version: {es_info['version']['number']}")
except Exception as e:
    print(f"Error connecting to Elasticsearch: {str(e)}")
    indices = []
else:
    # Get all indices with relevant stats (using pri.store.size and docs.count)
    try:
        indices = es.cat.indices(format="json", h="index,pri.store.size,docs.count,creation.date.string")
    except Exception as e:
        print(f"Error fetching indices: {str(e)}")
        indices = []

# Collect index information
results = []
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
        phase = index_ilm.get("phase", "unknown")
        
        # Get creation date
        creation_date_str = idx.get("creation.date.string", "")
        creation_date = "unknown"
        if creation_date_str:
            try:
                # Replace Z with +00:00 for ISO format
                dt_str = creation_date_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
                creation_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                print(f"Warning: Could not parse creation date '{creation_date_str}' for index '{index_name}'")
        
        # Get document count
        doc_count = int(idx.get("docs.count", "0"))
        
        results.append({
            "index": index_name,
            "policy": policy,
            "phase": phase,
            "size": format_size(size_bytes),
            "size_bytes": size_bytes,
            "creation_date": creation_date,
            "doc_count": doc_count
        })

# Prepare CSV rows
csv_rows = [
    {
        "Index": r["index"],
        "Policy": r["policy"],
        "Phase": r["phase"],
        "Size": r["size"],
        "Size (Bytes)": r["size_bytes"],
        "Creation Date": r["creation_date"],
        "Document Count": r["doc_count"]
    }
    for r in results
]

# Output results to JSON file
try:
    with open(json_output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Results written to {json_output_file}")
except Exception as e:
    print(f"Error writing to JSON file '{json_output_file}': {str(e)}")

# Output results to CSV file
try:
    df = pd.DataFrame(csv_rows)
    df.to_csv(csv_output_file, index=False)
    print(f"Results written to {csv_output_file}")
except Exception as e:
    print(f"Error writing to CSV file '{csv_output_file}': {str(e)}")
