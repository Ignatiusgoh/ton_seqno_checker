#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Find and replace the extract_pairs_seqno_txid function
old_function = '''def extract_pairs_seqno_txid(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        # metadata.requestContext.txId from log object
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
    return pairs'''

new_function = '''def extract_pairs_seqno_txid(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
    return pairs'''

# Replace the function
new_content = content.replace(old_function, new_function)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Updated extract_pairs_seqno_txid function")
