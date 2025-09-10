#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    lines = f.readlines()

# Find the line with the JSON parsing error
for i, line in enumerate(lines):
    if 'return resp.json()' in line:
        # Replace the entire try-except block
        start_line = i - 2  # Go back to the try line
        end_line = i + 4    # Go to the raise line
        
        # Create the new code
        new_code = '''    # Parse multiple JSON objects from response
    try:
        # Split response by newlines and try to parse each line
        response_lines = resp.text.strip().split('\\n')
        json_objects = []
        
        for line in response_lines:
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
        raise
'''
        
        # Replace the lines
        lines[start_line:end_line+1] = [new_code + '\n']
        break

# Write the updated file
with open('automation.py', 'w') as f:
    f.writelines(lines)

print("Patched automation.py with JSON parsing fix")
