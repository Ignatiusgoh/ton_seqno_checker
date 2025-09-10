#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Replace the JSON parsing section in request_dataprime
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

# Replace the section
new_content = content.replace(old_section, new_section)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Updated automation.py with targeted JSON parsing fix")
