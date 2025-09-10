#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Add debugging to print userData content
old_debug_section = '''        print(f"Processing log {i+1}: keys = {list(lg.keys())}")'''

new_debug_section = '''        print(f"Processing log {i+1}: keys = {list(lg.keys())}")
        user_data = lg.get("userData")
        if user_data:
            print(f"  Log {i+1} userData: {user_data[:200]}...")
        else:
            print(f"  Log {i+1}: no userData field")'''

# Replace the section
new_content = content.replace(old_debug_section, new_debug_section)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Added userData debugging")
