#!/usr/bin/env python3
import json

# Simulate the log extraction process
def extract_message_field(log):
    # Try common message/text fields
    for key in ("message", "text", "body", "content", "log", "msg"):
        val = log.get(key)
        if isinstance(val, str):
            return val
    # Sometimes message is nested
    # Try to join stringy values to search
    flattened = []
    def _collect_strings(node):
        if isinstance(node, str):
            flattened.append(node)
        elif isinstance(node, dict):
            for v in node.values():
                _collect_strings(v)
        elif isinstance(node, list):
            for v in node:
                _collect_strings(v)
    _collect_strings(log)
    if flattened:
        # Prefer lines containing 'enrichment object'
        for s in flattened:
            if "enrichment object" in s:
                return s
        return flattened[0]
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

# Test with the actual log structure
log = {
    "metadata": [{"key": "branchid", "value": "96ba802d-e6d3-6661-f969-ea852078dbd6"}],
    "userData": "{\"message\":\"enrichment object: {\\\"enrichTransaction\\\":{\\\"data\\\":{\\\"seqno\\\":194212,\\\"isDeployed\\\":true,\\\"balance\\\":\\\"11472525004222\\\"},\\\"timestamp\\\":1756567137061}}\"}"
}

print("Log structure:")
print(f"Keys: {list(log.keys())}")
print(f"userData: {log.get('userData', 'Not found')}")

# Extract message
msg = extract_message_field(log)
print(f"Extracted message: {msg}")

# Try to parse JSON
parsed = extract_json_after_label_from_text(msg or "")
print(f"Parsed JSON: {parsed}")

if parsed:
    seq = parsed.get("enrichTransaction", {}).get("data", {}).get("seqno")
    print(f"Extracted seqno: {seq}")
else:
    print("Failed to parse JSON from message")
