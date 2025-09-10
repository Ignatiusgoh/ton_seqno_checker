#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Replace the parse_multiple_json_objects function with a simpler one
old_function = '''def parse_multiple_json_objects(text: str) -> List[Dict[str, Any]]:
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
    
    return objects'''

new_function = '''def parse_multiple_json_objects(text: str) -> List[Dict[str, Any]]:
    """Parse multiple JSON objects from a single response string"""
    objects = []
    
    # Split by newlines and try to parse each line as JSON
    lines = text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):  # Only add dict objects
                objects.append(obj)
        except json.JSONDecodeError:
            # If line parsing fails, try to find JSON objects in the line
            decoder = json.JSONDecoder()
            idx = 0
            while idx < len(line):
                try:
                    obj, end_idx = decoder.raw_decode(line, idx)
                    if isinstance(obj, dict):
                        objects.append(obj)
                    idx = end_idx
                except json.JSONDecodeError:
                    idx += 1
    
    return objects'''

# Replace the function
new_content = content.replace(old_function, new_function)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Updated automation.py with simpler JSON parsing fix")
