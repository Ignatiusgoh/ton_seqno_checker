#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Add debugging to print full userData content
old_debug_section = '''        if user_data:
            print(f"  Log {i+1} userData: {user_data[:200]}...")
        else:
            print(f"  Log {i+1}: no userData field")'''

new_debug_section = '''        if user_data:
            print(f"  Log {i+1} userData (full): {user_data}")
            # Try to parse and show structure
            try:
                import json
                user_data_obj = json.loads(user_data)
                print(f"  Log {i+1} parsed userData keys: {list(user_data_obj.keys())}")
                if "metadata" in user_data_obj:
                    print(f"  Log {i+1} metadata keys: {list(user_data_obj['metadata'].keys())}")
                    if "requestContext" in user_data_obj["metadata"]:
                        print(f"  Log {i+1} requestContext keys: {list(user_data_obj['metadata']['requestContext'].keys())}")
            except Exception as e:
                print(f"  Log {i+1} failed to parse userData: {e}")
        else:
            print(f"  Log {i+1}: no userData field")'''

# Replace the section
new_content = content.replace(old_debug_section, new_debug_section)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Added full userData debugging")
