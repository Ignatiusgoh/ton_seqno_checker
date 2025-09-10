#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Add the parse_multiple_json_objects function after the parse_utc_timestamp function
insert_point = content.find('def ensure_api_key() -> str:')
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
            objects.append(obj)
        except json.JSONDecodeError as e:
            print(f"Failed to parse line: {repr(line[:50])}... Error: {e}")
    
    return objects


'''

# Insert the new function
new_content = content[:insert_point] + new_function + content[insert_point:]

# Update the request_dataprime function to use the new parsing
old_json_parse = '''    # Try to parse JSON with better error handling
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        print(f"JSON Parse Error: {e}")
        print(f"Full response text: {resp.text}")
        print("This usually means the API returned an error page instead of JSON")
        raise'''

new_json_parse = '''    # Parse multiple JSON objects
    try:
        json_objects = parse_multiple_json_objects(resp.text)
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
new_content = new_content.replace(old_json_parse, new_json_parse)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Updated automation.py with JSON parsing fix")
