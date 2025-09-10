#!/usr/bin/env python3
import json

# Test the complete flow with a simple example
def extract_message_field(log):
    # Check userData field which contains JSON with message
    user_data = log.get("userData")
    if user_data and isinstance(user_data, str):
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

# Test with the actual log structure
log = {
    "metadata": [{"key": "branchid", "value": "96ba802d-e6d3-6661-f969-ea852078dbd6"}],
    "userData": '{"class":"fireblocks::serialization_service::parseEnrichment","message":"enrichment object: {\\"enrichTransaction\\":{\\"data\\":{\\"seqno\\":194212,\\"isDeployed\\":true,\\"balance\\":\\"11472525004222\\"},\\"timestamp\\":1756567137061}}","metadata":{"requestContext":{"asset":"TON","coreFingerprint":"a93b330a2f06cf75","endpoint":"serializeAndSendForSigning","tenantId":"d41ac52c-b996-5ead-b920-bc6e2e1d75c1","txId":"9b13f109-7223-43d7-b960-b33bd1d7baa2","userId":"c68af909-5bce-6340-c2e5-a39d4181cd48"}}}'
}

print("Testing complete extraction flow...")

# Step 1: Extract message
msg = extract_message_field(log)
print(f"1. Message: {msg}")

if not msg:
    print("❌ No message found")
    exit(1)

# Step 2: Parse JSON
parsed = extract_json_after_label_from_text(msg)
print(f"2. Parsed: {parsed}")

if not parsed:
    print("❌ Failed to parse JSON")
    exit(1)

# Step 3: Extract seqno
seq = deep_get(parsed, ["enrichTransaction", "data", "seqno"])
print(f"3. Seqno: {seq} (type: {type(seq)})")

if isinstance(seq, str) and seq.isdigit():
    seq = int(seq)

if not isinstance(seq, int):
    print("❌ Seqno is not int")
    exit(1)

# Step 4: Extract txId
user_data = log.get("userData")
if user_data and isinstance(user_data, str):
    try:
        user_data_obj = json.loads(user_data)
        txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
        print(f"4. TxId: {txid}")
    except json.JSONDecodeError as e:
        print(f"4. Failed to parse userData: {e}")

print("✅ All steps completed successfully!")
