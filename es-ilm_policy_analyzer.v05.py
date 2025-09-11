import json
import pandas as pd
from elasticsearch import Elasticsearch
import getpass
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

# Prompt for Elasticsearch connection details
host = input("Enter Elasticsearch host (e.g., https://localhost:9200): ")
username = input("Enter username: ")
password = getpass.getpass("Enter password: ")
# Prompt for size threshold
size_threshold_str = input("Enter size threshold (e.g., 1gb, 500mb): ")
# Prompt for output files
json_output_file = input("Enter JSON output file path (e.g., output.json): ")
csv_output_file = input("Enter CSV output file path (e.g., output.csv): ")

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
    print(f"Elasticsearch version: {es_info['version']['number']}")
except Exception as e:
    print(f"Error connecting to Elasticsearch: {str(e)}")
    indices = []
else:
    # Get all indices with relevant stats (using pri.store.size for primary size check)
    try:
        indices = es.cat.indices(format="json", h="index,pri.store.size,pri,rep,creation.date.string")
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
            phase = index_ilm.get("phase", "unknown")
            
            # Get shard counts
            pri_shards = int(idx.get("pri", "0"))
            rep_shards = int(idx.get("rep", "0"))
            total_shards = pri_shards * (1 + rep_shards)
            
            # Get creation date and month
            creation_date_str = idx.get("creation.date.string", "")
            creation_month = "unknown"
            creation_date = "unknown"
            if creation_date_str:
                try:
                    # Replace Z with +00:00 for ISO format
                    dt_str = creation_date_str.replace('Z', '+00:00')
                    dt = datetime.fromisoformat(dt_str)
                    creation_month = dt.strftime("%Y-%m")
                    creation_date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    print(f"Warning: Could not parse creation date '{creation_date_str}' for index '{index_name}'")
            
            groups[policy].append({
                "index": index_name,
                "size_bytes": size_bytes,
                "size_readable": format_size(size_bytes),
                "total_shards": total_shards,
                "phase": phase,
                "creation_month": creation_month,
                "creation_date": creation_date,
                "creation_date_raw": creation_date_str
            })

# Fetch all ILM policies
policy_settings = {}
try:
    # Get all ILM policies
    all_policies_response = es.ilm.get_lifecycle()
    # Convert ObjectApiResponse to dict
    all_policies = all_policies_response.body if hasattr(all_policies_response, 'body') else dict(all_policies_response)
    print(f"Full ILM policies response: {json.dumps(all_policies, indent=2)}")
    
    for policy in groups.keys():
        print(f"Processing policy: {policy}")
        if policy not in all_policies:
            print(f"Warning: Policy '{policy}' not found in Elasticsearch")
            policy_settings[policy] = {"error": "Policy not found"}
            continue
        
        policy_def = all_policies.get(policy, {})
        inner_policy = policy_def.get('policy', policy_def)  # Handle nested or direct policy structure
        phases_def = inner_policy.get('phases', {})
        print(f"Phases for policy '{policy}': {json.dumps(phases_def, indent=2)}")
        
        rollover_settings = {}
        has_rollover = False
        for phase, config in phases_def.items():
            actions = config.get('actions', {})
            rollover = actions.get('rollover', {"note": "No rollover settings defined"})
            if "note" not in rollover:
                has_rollover = True
            phase_settings = {
                "lifetime": config.get('min_age', 'Not specified'),
                "rollover": rollover,
                "num_indices": 0  # Will be updated later if indices exist
            }
            rollover_settings[phase] = phase_settings
            print(f"Phase '{phase}' settings: lifetime={phase_settings['lifetime']}, rollover={json.dumps(rollover)}")
        
        policy_settings[policy] = rollover_settings
        if not has_rollover:
            print(f"Warning: No rollover settings found for any phase in policy '{policy}'")
            policy_settings[policy]["note"] = "No phases with rollover settings"
except Exception as e:
    print(f"Error: Failed to get ILM policies: {str(e)}")
    for policy in groups.keys():
        policy_settings[policy] = {"error": str(e)}

# Calculate stats per group and prepare CSV data
results = {}
csv_rows = []
for policy, idx_list in groups.items():
    if not idx_list:
        continue
    num_indices = len(idx_list)
    total_shards = sum(i["total_shards"] for i in idx_list)
    total_size_bytes = sum(i["size_bytes"] for i in idx_list)
    
    # Group by phase
    phase_groups = defaultdict(list)
    for i in idx_list:
        phase_groups[i["phase"]].append(i)
    
    phases = {}
    for phase, plist in phase_groups.items():
        p_num = len(plist)
        p_size_bytes = sum(p["size_bytes"] for p in plist)
        p_shards = sum(p["total_shards"] for p in plist)
        phases[phase] = {
            "num_indices": p_num,
            "total_shards": p_shards,
            "total_size": format_size(p_size_bytes),
            "total_size_bytes": p_size_bytes,
            "indices": [
                {"name": p["index"], "size": p["size_readable"], "shards": p["total_shards"], "creation_date": p["creation_date"]}
                for p in plist
            ]
        }
        # Update num_indices in phase_settings
        if phase in policy_settings.get(policy, {}):
            policy_settings[policy][phase]["num_indices"] = p_num
    
    # Monthly breakdown
    monthly_sizes = defaultdict(float)
    monthly_counts = defaultdict(int)
    for i in idx_list:
        month = i["creation_month"]
        if month != "unknown":
            monthly_sizes[month] += i["size_bytes"]
            monthly_counts[month] += 1
    
    monthly_breakdown = {
        month: {
            "num_indices": monthly_counts[month],
            "size": format_size(size),
            "size_bytes": size
        } for month, size in sorted(monthly_sizes.items())
    }
    
    # Daily breakdown
    daily_sizes = defaultdict(float)
    daily_counts = defaultdict(int)
    for i in idx_list:
        date = i["creation_date"]
        if date != "unknown":
            daily_sizes[date] += i["size_bytes"]
            daily_counts[date] += 1
    
    daily_breakdown = {
        date: {
            "num_indices": daily_counts[date],
            "size": format_size(size),
            "size_bytes": size
        } for date, size in sorted(daily_sizes.items())
    }
    
    results[policy] = {
        "num_indices": num_indices,
        "total_shards": total_shards,
        "total_size": format_size(total_size_bytes),
        "total_size_bytes": total_size_bytes,
        "phases": phases,
        "monthly_breakdown": monthly_breakdown,
        "daily_breakdown": daily_breakdown,
        "phase_settings": policy_settings.get(policy, {"error": "No settings retrieved"})
    }
    
    # Prepare CSV rows
    # Policy-level row
    csv_rows.append({
        "Policy": policy,
        "Num Indices": num_indices,
        "Total Shards": total_shards,
        "Total Size": format_size(total_size_bytes),
        "Total Size (Bytes)": total_size_bytes,
        "Phase": "",
        "Phase Num Indices": "",
        "Phase Lifetime": "",
        "Phase Rollover": "",
        "Month": "",
        "Month Num Indices": "",
        "Month Size": "",
        "Month Size (Bytes)": "",
        "Date": "",
        "Date Num Indices": "",
        "Date Size": "",
        "Date Size (Bytes)": ""
    })
    
    # Phase settings rows
    phase_settings = policy_settings.get(policy, {"error": "No settings retrieved"})
    if "error" not in phase_settings and "note" not in phase_settings:
        for phase, settings in phase_settings.items():
            csv_rows.append({
                "Policy": policy,
                "Num Indices": "",
                "Total Shards": "",
                "Total Size": "",
                "Total Size (Bytes)": "",
                "Phase": phase,
                "Phase Num Indices": settings["num_indices"],
                "Phase Lifetime": settings["lifetime"],
                "Phase Rollover": json.dumps(settings["rollover"]),
                "Month": "",
                "Month Num Indices": "",
                "Month Size": "",
                "Month Size (Bytes)": "",
                "Date": "",
                "Date Num Indices": "",
                "Date Size": "",
                "Date Size (Bytes)": ""
            })
    elif "note" in phase_settings:
        csv_rows.append({
            "Policy": policy,
            "Num Indices": "",
            "Total Shards": "",
            "Total Size": "",
            "Total Size (Bytes)": "",
            "Phase": "",
            "Phase Num Indices": "",
            "Phase Lifetime": "",
            "Phase Rollover": phase_settings["note"],
            "Month": "",
            "Month Num Indices": "",
            "Month Size": "",
            "Month Size (Bytes)": "",
            "Date": "",
            "Date Num Indices": "",
            "Date Size": "",
            "Date Size (Bytes)": ""
        })
    elif "error" in phase_settings:
        csv_rows.append({
            "Policy": policy,
            "Num Indices": "",
            "Total Shards": "",
            "Total Size": "",
            "Total Size (Bytes)": "",
            "Phase": "",
            "Phase Num Indices": "",
            "Phase Lifetime": "",
            "Phase Rollover": phase_settings["error"],
            "Month": "",
            "Month Num Indices": "",
            "Month Size": "",
            "Month Size (Bytes)": "",
            "Date": "",
            "Date Num Indices": "",
            "Date Size": "",
            "Date Size (Bytes)": ""
        })
    
    # Monthly breakdown rows
    for month, data in monthly_breakdown.items():
        csv_rows.append({
            "Policy": policy,
            "Num Indices": "",
            "Total Shards": "",
            "Total Size": "",
            "Total Size (Bytes)": "",
            "Phase": "",
            "Phase Num Indices": "",
            "Phase Lifetime": "",
            "Phase Rollover": "",
            "Month": month,
            "Month Num Indices": data["num_indices"],
            "Month Size": data["size"],
            "Month Size (Bytes)": data["size_bytes"],
            "Date": "",
            "Date Num Indices": "",
            "Date Size": "",
            "Date Size (Bytes)": ""
        })
    
    # Daily breakdown rows
    for date, data in daily_breakdown.items():
        csv_rows.append({
            "Policy": policy,
            "Num Indices": "",
            "Total Shards": "",
            "Total Size": "",
            "Total Size (Bytes)": "",
            "Phase": "",
            "Phase Num Indices": "",
            "Phase Lifetime": "",
            "Phase Rollover": "",
            "Month": "",
            "Month Num Indices": "",
            "Month Size": "",
            "Month Size (Bytes)": "",
            "Date": date,
            "Date Num Indices": data["num_indices"],
            "Date Size": data["size"],
            "Date Size (Bytes)": data["size_bytes"]
        })

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
