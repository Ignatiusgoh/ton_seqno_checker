#!/usr/bin/env python3

# Read the current automation.py file
with open('automation.py', 'r') as f:
    content = f.read()

# Fix the regex pattern
old_regex = r'm = re.search(r"metadata\.requestContext\.txId[\"\'']?\s*[:=]\s*[\"\'']?([0-9a-fA-F-]{20,})", text)'
new_regex = r'm = re.search(r"metadata\\.requestContext\\.txId[\\"\'\\']?\\s*[:=]\\s*[\\"\'\\']?([0-9a-fA-F-]{20,})", text)'

# Replace the regex
new_content = content.replace(old_regex, new_regex)

# Write the updated content back
with open('automation.py', 'w') as f:
    f.write(new_content)

print("Fixed regex pattern")
