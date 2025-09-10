#!/usr/bin/env python3
import json

# Test the pair extraction logic
def extract_message_field(log):
    # Try common message/text fields
    for key in ("message", "text", "body", "content", "log", "msg"):
        val = log.get(key)
        if isinstance(val, str):
            return val
    
    # Check userData field which contains JSON with message
    user_data = log.get("userData")
    if isinstance(user_data, str):
        try:
            user_data_obj = json.loads(user_data)
            if isinstance(user_data_obj, dict):
                message = user_data_obj.get("message")
                if isinstance(message, str):
                    return message
        except json.JSONDecodeError:
            pass
    
    return None

def extract_json_after_label_from_text(text, label="enrichment object:"):
    if not isinstance(text, str):
        return None
    idx = text.find(label)
    start = 0 if idx == -1 else idx + len(label)
    brace = text.find("{", start)
    if brace == -1:
        return None
    snippet = text[brace:]
    # Try to balance braces roughly
    depth = 0
    end_idx = None
    for i, ch in enumerate(snippet):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    if end_idx is None:
        return None
    json_str = snippet[:end_idx].strip()
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None

def deep_get(obj, keys, default=None):
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def find_key_recursive(obj, target_path):
    if not isinstance(obj, dict):
        return None
    def _walk(node, path):
        if not path:
            return node
        if isinstance(node, dict):
            head, tail = path[0], path[1:]
            if head in node:
                return _walk(node[head], tail)
            # search all children for potential subpath
            for v in node.values():
                found = _walk(v, path)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for it in node:
                found = _walk(it, path)
                if found is not None:
                    return found
        return None
    return _walk(obj, target_path)

# Test with a sample log from the second query
log = {
    "metadata": [{"key": "branchid", "value": "96ba802d-e6d3-6661-f969-ea852078dbd6"}],
    "userData": "{\"message\":\"enrichment object: {\\\"enrichTransaction\\\":{\\\"data\\\":{\\\"seqno\\\":194212,\\\"isDeployed\\\":true,\\\"balance\\\":\\\"11472525004222\\\"},\\\"timestamp\\\":1756567137061}}\",\"metadata\":{\"requestContext\":{\"asset\":\"TON\",\"coreFingerprint\":\"a93b330a2f06cf75\",\"endpoint\":\"serializeAndSendForSigning\",\"tenantId\":\"d41ac52c-b996-5ead-b920-bc6e2e1d75c1\",\"txId\":\"9b13f109-7223-43d7-b960-b33bd1d7baa2\",\"userId\":\"c68af909-5bce-6340-c2e5-a39d4181cd48\"}}}"
}

print("Testing pair extraction...")
print(f"Log keys: {list(log.keys())}")

# Extract message
msg = extract_message_field(log)
print(f"Extracted message: {msg}")

# Parse JSON
parsed = extract_json_after_label_from_text(msg or "")
print(f"Parsed JSON: {parsed}")

if parsed:
    seq = deep_get(parsed, ["enrichTransaction", "data", "seqno"])
    print(f"Extracted seqno: {seq}")
    
    # Try to get txId from userData
    user_data = log.get("userData")
    if user_data:
        try:
            user_data_obj = json.loads(user_data)
            txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
            print(f"Extracted txId from userData: {txid}")
        except json.JSONDecodeError as e:
            print(f"Failed to parse userData: {e}")
else:
    print("Failed to parse JSON from message")
