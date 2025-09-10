#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Add debugging to the extract_pairs_seqno_txid function
old_section = '''def extract_pairs_seqno_txid(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pairs: List[Dict[str, Any]] = []
    for lg in logs:'''

new_section = '''def extract_pairs_seqno_txid(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pairs: List[Dict[str, Any]] = []
    print(f"Processing {len(logs)} logs for pair extraction...")
    for i, lg in enumerate(logs):
        print(f"Processing log {i+1}: keys = {list(lg.keys())}")'''

# Replace the section
new_content = content.replace(old_section, new_section)

# Add more debugging after the seqno extraction
old_seqno_check = '''        if not isinstance(seq, int):
            continue'''

new_seqno_check = '''        if not isinstance(seq, int):
            print(f"  Log {i+1}: seqno is not int: {seq} (type: {type(seq)})")
            continue
        print(f"  Log {i+1}: found seqno: {seq}")'''

# Replace the section
new_content = new_content.replace(old_seqno_check, new_seqno_check)

# Add debugging for txId extraction
old_txid_check = '''        if txid is None:
            continue'''

new_txid_check = '''        if txid is None:
            print(f"  Log {i+1}: no txId found")
            continue
        print(f"  Log {i+1}: found txId: {txid}")'''

# Replace the section
new_content = new_content.replace(old_txid_check, new_txid_check)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Added debugging to extract_pairs_seqno_txid function")
