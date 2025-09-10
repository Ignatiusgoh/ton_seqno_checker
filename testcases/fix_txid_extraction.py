#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Fix the txId extraction logic
old_txid_section = '''        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
            except json.JSONDecodeError:
                pass'''

new_txid_section = '''        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
                print(f"  Log {i+1}: found txId in userData: {txid}")
            except json.JSONDecodeError as e:
                print(f"  Log {i+1}: failed to parse userData: {e}")'''

# Replace the section
new_content = content.replace(old_txid_section, new_txid_section)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Fixed txId extraction logic")
