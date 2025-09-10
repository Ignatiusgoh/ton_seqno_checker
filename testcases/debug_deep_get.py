#!/usr/bin/env python3

# Test the deep_get function
def deep_get(obj, keys, default=None):
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

# Test with the actual userData structure
user_data_str = '{"class":"fireblocks::serialization_service::parseEnrichment","class1":"","component":"serialization_service","container":{"id":"bf2a826d249ca2f9e38c57fbf182b9e4a7de6b262ce30f9b2d5eac360fefb998","image":{"name":"registry.gitlab.com/fireblocks/build-docker/prod/core-serialization_service:49fb9cea"},"name":"serialization-service","pod":"serialization-service-6f6d7ff66-ps8q8"},"file":"enclave/SerializationServiceRPCServer.cpp","file_line":"1037","hostname":"aks-usprodz3np-39013279-vmss00001t","message":"enrichment object: {\\"enrichTransaction\\":{\\"data\\":{\\"seqno\\":194212,\\"isDeployed\\":true,\\"balance\\":\\"11472525004222\\"},\\"timestamp\\":1756567137061}}","message_level":"INFO","metadata":{"requestContext":{"asset":"TON","coreFingerprint":"a93b330a2f06cf75","endpoint":"serializeAndSendForSigning","tenantId":"d41ac52c-b996-5ead-b920-bc6e2e1d75c1","txId":"9b13f109-7223-43d7-b960-b33bd1d7baa2","userId":"c68af909-5bce-6340-c2e5-a39d4181cd48"}},"namespace":"us-prod","offset":10328125,"shipper":"vector-aggregator","stream":"stdout","thread":"33","time":"30/08/2025 15:18:57,173"}'

import json
user_data_obj = json.loads(user_data_str)

print("Testing deep_get function...")
print(f"userData keys: {list(user_data_obj.keys())}")
print(f"metadata key exists: {'metadata' in user_data_obj}")
print(f"metadata value: {user_data_obj['metadata']}")
print(f"requestContext key exists: {'requestContext' in user_data_obj['metadata']}")
print(f"requestContext value: {user_data_obj['metadata']['requestContext']}")
print(f"txId key exists: {'txId' in user_data_obj['metadata']['requestContext']}")
print(f"txId value: {user_data_obj['metadata']['requestContext']['txId']}")

# Test deep_get
result = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
print(f"deep_get result: {result}")

# Test step by step
step1 = deep_get(user_data_obj, ["metadata"])
print(f"Step 1 (metadata): {step1}")
step2 = deep_get(step1, ["requestContext"])
print(f"Step 2 (requestContext): {step2}")
step3 = deep_get(step2, ["txId"])
print(f"Step 3 (txId): {step3}")
