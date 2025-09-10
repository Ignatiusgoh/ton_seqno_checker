#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Remove all the debugging code and create a clean version
# Find the extract_pairs_seqno_txid function and replace it with a clean version
import re

# Find the function start
start_pattern = r'def extract_pairs_seqno_txid\(logs: List\[Dict\[str, Any\]\]\) -> List\[Dict\[str, Any\]\]:'
start_match = re.search(start_pattern, content)
if not start_match:
    print("Could not find function start")
    exit(1)

start_pos = start_match.start()

# Find the next function definition
next_func_pattern = r'\n\ndef \w+\(.*\) -> .*:'
next_match = re.search(next_func_pattern, content[start_pos + 1:])
if next_match:
    end_pos = start_pos + 1 + next_match.start()
else:
    end_pos = len(content)

# Create the clean function
clean_function = '''def extract_pairs_seqno_txid(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pairs: List[Dict[str, Any]] = []
    for lg in logs:
        msg = extract_message_field(lg)
        parsed = extract_json_after_label_from_text(msg or "")
        if not parsed:
            try:
                parsed = json.loads(msg) if msg else None
            except Exception:
                parsed = None
        if not parsed:
            continue
        seq = deep_get(parsed, ["enrichTransaction", "data", "seqno"])
        if isinstance(seq, str) and seq.isdigit():
            seq = int(seq)
        if not isinstance(seq, int):
            continue
        
        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
            except json.JSONDecodeError:
                pass
        
        # If not found in userData, try other locations
        if txid is None:
            txid = deep_get(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            txid = find_key_recursive(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            # sometimes it's flattened as "metadata.requestContext.txId"
            txid = lg.get("metadata.requestContext.txId")
        if txid is None:
            # As last resort, scan strings that look like 'metadata.requestContext.txId:<uuid>'
            text = json.dumps(lg, ensure_ascii=False)
            m = re.search(r"metadata\.requestContext\.txId[\"']?\s*[:=]\s*[\"']?([0-9a-fA-F-]{20,})", text)
            txid = m.group(1) if m else None
        if txid is None:
            continue
        pairs.append({"Seqno": seq, "metadata.requestContext.txId": str(txid)})
    return pairs
'''

# Replace the function
new_content = content[:start_pos] + clean_function + content[end_pos:]

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Created clean version of extract_pairs_seqno_txid function")
