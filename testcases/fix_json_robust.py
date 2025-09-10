#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Add a more robust JSON parsing function
insert_point = content.find('def ensure_api_key() -> str:')
new_function = '''def parse_multiple_json_objects(text: str) -> List[Dict[str, Any]]:
    """Parse multiple JSON objects from a single response string"""
    objects = []
    
    # Try to parse the entire response as a single JSON first
    try:
        single_obj = json.loads(text)
        return [single_obj]
    except json.JSONDecodeError:
        pass
    
    # If that fails, try to find JSON objects by looking for complete braces
    decoder = json.JSONDecoder()
    idx = 0
    text = text.strip()
    
    while idx < len(text):
        # Skip whitespace
        while idx < len(text) and text[idx].isspace():
            idx += 1
        
        if idx >= len(text):
            break
            
        try:
            obj, end_idx = decoder.raw_decode(text, idx)
            objects.append(obj)
            idx = end_idx
        except json.JSONDecodeError:
            # If we can't parse from this position, try the next character
            idx += 1
    
    return objects


'''

# Insert the new function
new_content = content[:insert_point] + new_function + content[insert_point:]

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Updated automation.py with robust JSON parsing fix")
