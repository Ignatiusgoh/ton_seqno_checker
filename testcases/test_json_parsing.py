#!/usr/bin/env python3
import json

def parse_coralogix_response(text: str):
    """Parse Coralogix response that contains multiple JSON objects"""
    objects = []
    
    # Split by newlines and try to parse each line
    lines = text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                objects.append(obj)
        except json.JSONDecodeError:
            continue
    
    print(f"Parsed {len(objects)} JSON objects")
    
    # Find the object with results
    for obj in objects:
        if "result" in obj:
            return obj
    
    # If no result object found, return the first object
    return objects[0] if objects else {}

# Test with the actual response format
test_response = '''{"queryId":{"queryId":"1843d2ec-97d6-467e-835f-d53b422265fb"}}
{"result":{"results":[{"metadata":[{"key":"branchid","value":"96ba802d-e6d3-6661-f969-ea852078dbd6"}],"userData":"{\\"message\\":\\"enrichment object: {\\\\\\"enrichTransaction\\\\\\":{\\\\\\"data\\\\\\":{\\\\\\"seqno\\\\\\":194212,\\\\\\"isDeployed\\\\\\":true,\\\\\\"balance\\\\\\":\\\\\\"11472525004222\\\\\\"},\\\\\\"timestamp\\\\\\":1756567137061}}\\"}"}]}}'''

print("Testing JSON parsing...")
result = parse_coralogix_response(test_response)
print(f"Result keys: {list(result.keys())}")

if "result" in result:
    results = result["result"].get("results", [])
    print(f"Number of results: {len(results)}")
    if results:
        user_data = results[0].get("userData", "")
        print(f"UserData contains seqno: {'seqno' in user_data}")
        print(f"UserData contains enrichment: {'enrichment object' in user_data}")
        
        # Try to extract the seqno
        if "seqno" in user_data:
            import re
            match = re.search(r'"seqno":(\d+)', user_data)
            if match:
                seqno = int(match.group(1))
                print(f"Extracted seqno: {seqno}")
