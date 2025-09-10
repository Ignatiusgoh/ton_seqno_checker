#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Add more detailed debugging
old_debug_section = '''        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
                print(f"  Log {i+1}: found txId in userData: {txid}")
            except json.JSONDecodeError as e:
                print(f"  Log {i+1}: failed to parse userData: {e}")'''

new_debug_section = '''        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        print(f"  Log {i+1}: user_data type: {type(user_data)}, length: {len(user_data) if user_data else 0}")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                print(f"  Log {i+1}: parsed userData successfully")
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
                print(f"  Log {i+1}: deep_get result: {txid}")
                if txid:
                    print(f"  Log {i+1}: found txId in userData: {txid}")
                else:
                    print(f"  Log {i+1}: deep_get returned None")
            except json.JSONDecodeError as e:
                print(f"  Log {i+1}: failed to parse userData: {e}")
        else:
            print(f"  Log {i+1}: user_data is not a string or is None")'''

# Replace the section
new_content = content.replace(old_debug_section, new_debug_section)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Added detailed debugging")
