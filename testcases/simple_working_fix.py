#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Fix 1: Update JSON parsing - replace the problematic section
old_section = '''    # Try to parse JSON with better error handling
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        print(f"JSON Parse Error: {e}")
        print(f"Full response text: {resp.text}")
        print("This usually means the API returned an error page instead of JSON")
        raise'''

new_section = '''    # Parse multiple JSON objects from response
    try:
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
        
        return json_objects[0] if json_objects else {}
        
    except Exception as e:
        print(f"Error parsing response: {e}")
        raise'''

# Replace the section
new_content = content.replace(old_section, new_section)

# Fix 2: Update extract_message_field to handle userData
old_message_func = '''    # Try common message/text fields
    for key in ("message", "text", "body", "content", "log", "msg"):
        val = log.get(key)
        if isinstance(val, str):
            return val'''

new_message_func = '''    # Try common message/text fields
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
            pass'''

# Replace the section
new_content = new_content.replace(old_message_func, new_message_func)

# Fix 3: Update txId extraction to use userData first
old_txid_section = '''        # metadata.requestContext.txId from log object
        txid = deep_get(lg, ["metadata", "requestContext", "txId"])'''

new_txid_section = '''        # Try to get txId from userData first
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
            txid = deep_get(lg, ["metadata", "requestContext", "txId"])'''

# Replace the section
new_content = new_content.replace(old_txid_section, new_txid_section)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Applied working fixes")
