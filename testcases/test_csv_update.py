#!/usr/bin/env python3

# Test the CSV update logic with the known completed transaction IDs
completed_txids = [
    'b96e8f19-da89-49d4-832e-6692f2fd0046',
    'b6a62d8a-dff4-425c-92cf-6e02ce31e18a', 
    '74a707cf-a93a-4dd5-b243-4dd8d2f62920'
]

# Read the current CSV
import csv
rows = []
with open('seqno_txid.csv', 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print("Current CSV:")
for row in rows:
    print(f"{row['Seqno']},{row['metadata.requestContext.txId']},{row['Status']}")

# Create a set of completed txIds for faster lookup
completed_set = set(completed_txids)

# Group rows by seqno to find txIds with same seqno
seqno_groups = {}
for row in rows:
    seqno = int(row["Seqno"])
    if seqno not in seqno_groups:
        seqno_groups[seqno] = []
    seqno_groups[seqno].append(row)

# Update status for each row
for row in rows:
    txid = row["metadata.requestContext.txId"]
    seqno = int(row["Seqno"])
    
    if txid in completed_set:
        row["Status"] = "COMPLETED"
    else:
        # Check if any txId with the same seqno is completed
        same_seqno_rows = seqno_groups.get(seqno, [])
        has_completed_sibling = any(
            other_row["metadata.requestContext.txId"] in completed_set 
            for other_row in same_seqno_rows 
            if other_row["metadata.requestContext.txId"] != txid
        )
        
        if has_completed_sibling:
            row["Status"] = "SAFE_TO_FAIL"
        else:
            row["Status"] = "Unknown"

# Write updated CSV
fieldnames = ["Seqno", "metadata.requestContext.txId", "Status"]
with open('seqno_txid.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for row in rows:
        w.writerow(row)

print("\nUpdated CSV:")
for row in rows:
    print(f"{row['Seqno']},{row['metadata.requestContext.txId']},{row['Status']}")

print(f"\nStatus summary:")
print(f"  - COMPLETED: {len([r for r in rows if r['Status'] == 'COMPLETED'])}")
print(f"  - SAFE_TO_FAIL: {len([r for r in rows if r['Status'] == 'SAFE_TO_FAIL'])}")
print(f"  - Unknown: {len([r for r in rows if r['Status'] == 'Unknown'])}")
