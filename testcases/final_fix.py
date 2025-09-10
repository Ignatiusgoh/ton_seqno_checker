#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Fix 1: Update JSON parsing in request_dataprime
old_json_parse = '''    # Try to parse JSON with better error handling
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        print(f"JSON Parse Error: {e}")
        print(f"Full response text: {resp.text}")
        print("This usually means the API returned an error page instead of JSON")
        raise'''

new_json_parse = '''    # Parse multiple JSON objects from response
    try:
        # Split response by newlines and try to parse each line
        lines = resp.text.strip().split('\n')
        json_objects = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    json_objects.append(obj)
            except json.JSONDecodeError:
                continue
        
        print(f"Parsed {len(json_objects)} JSON objects from response")
        
        # Find the object with results
        for obj in json_objects:
            if "result" in obj:
                return obj
        
        # If no result object found, return the first object
        return json_objects[0] if json_objects else {}
        
    except Exception as e:
        print(f"Error parsing response: {e}")
        print(f"Full response text: {resp.text}")
        raise'''

# Replace the JSON parsing section
new_content = content.replace(old_json_parse, new_json_parse)

# Fix 2: Update extract_message_field to handle userData
old_extract_message = '''def extract_message_field(log: Dict[str, Any]) -> Optional[str]:
    # Try common message/text fields
    for key in ("message", "text", "body", "content", "log", "msg"):
        val = log.get(key)
        if isinstance(val, str):
            return val
    # Sometimes message is nested
    # Try to join stringy values to search
    flattened = []
    def _collect_strings(node: Any):
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
    return None'''

new_extract_message = '''def extract_message_field(log: Dict[str, Any]) -> Optional[str]:
    # Try common message/text fields
    for key in ("message", "text", "body", "content", "log", "msg"):
        val = log.get(key)
        if isinstance(val, str):
            return val
    
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
    
    # Sometimes message is nested
    # Try to join stringy values to search
    flattened = []
    def _collect_strings(node: Any):
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
    return None'''

# Replace the extract_message_field function
new_content = new_content.replace(old_extract_message, new_extract_message)

# Fix 3: Update extract_pairs_seqno_txid to use userData
old_extract_pairs = '''        # metadata.requestContext.txId from log object
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
            continue'''

new_extract_pairs = '''        # Try to get txId from userData first
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
            m = re.search(r"txId[\\"\\\'\']?\\s*[:=]\\s*[\\"\\\'\']?([0-9a-fA-F-]{20,})", text)
            txid = m.group(1) if m else None
        if txid is None:
            continue'''

# Replace the txId extraction section
new_content = new_content.replace(old_extract_pairs, new_extract_pairs)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Applied all fixes: JSON parsing, message extraction, and txId extraction")
