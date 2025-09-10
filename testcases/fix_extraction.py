#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Find and replace the extract_message_field function
old_function = '''def extract_message_field(log: Dict[str, Any]) -> Optional[str]:
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

new_function = '''def extract_message_field(log: Dict[str, Any]) -> Optional[str]:
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

# Replace the function
new_content = content.replace(old_function, new_function)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Updated extract_message_field function")
