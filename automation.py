#!/usr/bin/env python3

import os
import re
import sys
import json
import time
import csv
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DEFAULT_DP_URL = os.getenv("CORALOGIX_DP_URL", "https://api.coralogix.us/api/v1/dataprime/query")
API_KEY = os.getenv("CORALOGIX_API_KEY")


# Debug: Show if API key was loaded
if API_KEY:
    print(f"✅ API key loaded from environment: {API_KEY[:8]}...{API_KEY[-4:]}")
else:
    print("❌ No API key found in environment variables")
    print("Make sure you have set CORALOGIX_API_KEY in your .env file or environment")


def parse_utc_timestamp(value: str, is_after: bool = True) -> str:
    """
    Parse timestamp input and convert to UTC format.
    
    Args:
        value: Timestamp string in various formats
        is_after: True for 'after' timestamp (subtract 6 hours), False for 'before' (add 6 hours)
    
    Returns:
        UTC timestamp string in format "2025-01-20T00:00:00Z"
    """
    s = value.strip()
    
    # Handle UTC timestamp format: 2025-01-20T00:00:00Z
    if s.endswith('Z'):
        try:
            # Validate the UTC timestamp format
            dt = datetime.fromisoformat(s[:-1] + '+00:00')
            # Apply time adjustment
            if is_after:
                dt = dt - timedelta(hours=6)
            else:
                dt = dt + timedelta(hours=6)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    
    # Try other ISO-8601 variants and convert to UTC Z format
    s_with_tz = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s_with_tz)
        # Apply time adjustment
        if is_after:
            dt = dt - timedelta(hours=6)
        else:
            dt = dt + timedelta(hours=6)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    
    # Try a common format "YYYY-MM-DD HH:MM:SS" and assume UTC
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        # Apply time adjustment
        if is_after:
            dt = dt - timedelta(hours=6)
        else:
            dt = dt + timedelta(hours=6)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    
    # Handle 12-hour format: "Sep 7, 2025, 4:15:50 PM"
    try:
        # Parse the timestamp in user's local timezone
        dt = datetime.strptime(s, "%b %d, %Y, %I:%M:%S %p")
        
        # Get user's local timezone
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
        
        # Convert to UTC
        dt_utc = dt.astimezone(timezone.utc)
        
        # Apply time adjustment
        if is_after:
            dt_utc = dt_utc - timedelta(hours=6)
        else:
            dt_utc = dt_utc + timedelta(hours=6)
        
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    
    # Handle 24-hour format: "7 Sept 2025, 16:15:59"
    try:
        # Normalize month abbreviation (Sept -> Sep)
        normalized_s = s.replace("Sept", "Sep")
        
        # Parse the timestamp in user's local timezone
        dt = datetime.strptime(normalized_s, "%d %b %Y, %H:%M:%S")
        
        # Get user's local timezone
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
        
        # Convert to UTC
        dt_utc = dt.astimezone(timezone.utc)
        
        # Apply time adjustment
        if is_after:
            dt_utc = dt_utc - timedelta(hours=6)
        else:
            dt_utc = dt_utc + timedelta(hours=6)
        
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    
    # If all parsing attempts fail, provide helpful error message
    raise ValueError(f"Unrecognized time format: {value}. Expected formats: 'Sep 7, 2025, 4:15:50 PM', '7 Sept 2025, 16:15:59', or '2025-01-20T00:00:00Z'")


def ensure_api_key() -> str:
    if not API_KEY:
        print("ERROR: Please set CORALOGIX_API_KEY environment variable.", file=sys.stderr)
        sys.exit(1)
    return API_KEY


def escape_single_quotes(s: str) -> str:
    return s.replace("'", "\\'")


def build_first_query(tx_ids: List[str]) -> str:
    terms = []
    for tx in tx_ids:
        tx = tx.strip()
        if not tx:
            continue
        esc = escape_single_quotes(tx)
        terms.append(f"$d ~~ '{esc}' && $d ~~ 'seqno' && $d ~~ 'enrichment object'")
    if not terms:
        raise ValueError("No valid transaction IDs provided.")
    return "source logs | filter " + " || ".join(terms)


def build_second_query(tenant_id: str, seqnos: List[int]) -> str:
    tenant_id = escape_single_quotes(tenant_id.strip())
    parts = []
    for seq in seqnos:
        parts.append(f"($d ~~ '{tenant_id}' && $d ~~ 'seqno:{seq}'  && $d ~~ 'enrichment object')")
    if not parts:
        raise ValueError("No seqno values to build the second query.")
    return "source logs | filter " + " || ".join(parts)


def build_third_query(tx_ids: List[str]) -> str:
    """Build query to check completion status for transaction IDs."""
    parts = []
    for tx_id in tx_ids:
        tx_id = escape_single_quotes(tx_id.strip())
        if not tx_id:
            continue
        # Use the correct pattern: txId && status update to COMPLETED
        parts.append(f"($d ~~ '{tx_id}' && $d ~~ 'status update to COMPLETED')")
    if not parts:
        raise ValueError("No transaction IDs to build the third query.")
    return "source logs | filter " + " || ".join(parts)


def build_fourth_query(tx_ids: List[str]) -> str:
    """Build query to extract sourceId for transaction IDs."""
    parts = []
    for tx_id in tx_ids:
        tx_id = escape_single_quotes(tx_id.strip())
        if not tx_id:
            continue
        # Query pattern: txId && sourceId (simpler pattern that works)
        parts.append(f"($d ~~ '{tx_id}' && $d ~~ 'sourceId')")
    if not parts:
        raise ValueError("No transaction IDs to build the fourth query.")
    return "source logs | filter " + " || ".join(parts)


def request_dataprime(query: str, start_date: str, end_date: str) -> Dict[str, Any]:
    api_key = ensure_api_key()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "X-API-Key": api_key,
    }
    payload = {
        "query": query,
        "metadata": {
            "startDate": start_date,
            "endDate": end_date,
            "syntax": "QUERY_SYNTAX_DATAPRIME",
            "limit": 12000
        },
    }
    
    print(f"Making request to: {DEFAULT_DP_URL}")
    print(f"Query: {query}")
    print(f"Time range: {start_date} to {end_date}")
    
    resp = requests.post(DEFAULT_DP_URL, headers=headers, json=payload, timeout=60, verify=False)
    
    # Debug: Print response details
    print(f"Response Status Code: {resp.status_code}")
    print(f"Response Headers: {dict(resp.headers)}")
    print(f"Response Content-Type: {resp.headers.get('content-type', 'Not specified')}")
    print(f"Response Length: {len(resp.text)} characters")
    print(f"First 500 characters of response: {resp.text[:500]}")
    
    if resp.status_code == 403:
        print(f"403 Forbidden - Check your API key. Response: {resp.text}")
        print("Common causes:")
        print("1. Invalid or expired API key")
        print("2. Incorrect authentication method")
        print("3. Insufficient permissions")
        print("4. Wrong API endpoint")
        resp.raise_for_status()
    
    if resp.status_code != 200:
        print(f"HTTP Error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    
    # Parse multiple JSON objects from response
    try:
        lines = resp.text.strip().split("\n")
        json_objects = []
        
        for line in lines:
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
        
        return json_objects[0] if json_objects else {}
        
    except Exception as e:
        print(f"Error parsing response: {e}")
        raise


def extract_logs_from_response(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Try several common keys
    for key in ("data", "results", "logs", "records", "hits"):
        val = resp.get(key)
        if isinstance(val, list):
            return val
    # Some APIs wrap under 'result' or 'response'
    for key in ("result", "response"):
        val = resp.get(key)
        if isinstance(val, dict):
            for inner in ("data", "results", "logs", "records", "hits"):
                v2 = val.get(inner)
                if isinstance(v2, list):
                    return v2
    return []


def deep_get(obj: Any, keys: List[str], default: Any = None) -> Any:
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def find_key_recursive(obj: Any, target_path: List[str]) -> Optional[Any]:
    # Tries to find a nested path like ["metadata","requestContext","txId"] anywhere in the dict
    if not isinstance(obj, dict):
        return None
    def _walk(node: Any, path: List[str]) -> Optional[Any]:
        if not path:
            return node
        if isinstance(node, dict):
            head, tail = path[0], path[1:]
            if head in node:
                return _walk(node[head], tail)
            # search all children for potential subpath
            for v in node.values():
                found = _walk(v, path)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for it in node:
                found = _walk(it, path)
                if found is not None:
                    return found
        return None
    return _walk(obj, target_path)


def extract_json_after_label_from_text(text: str, label: str = "enrichment object:") -> Optional[Dict[str, Any]]:
    if not isinstance(text, str):
        return None
    idx = text.find(label)
    start = 0 if idx == -1 else idx + len(label)
    brace = text.find("{", start)
    if brace == -1:
        return None
    snippet = text[brace:]
    # Try to balance braces roughly
    depth = 0
    end_idx = None
    for i, ch in enumerate(snippet):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    if end_idx is None:
        return None
    json_str = snippet[:end_idx].strip()
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def extract_message_field(log: Dict[str, Any]) -> Optional[str]:
    # Try common message/text fields
    for key in ("message", "text", "body", "content", "log", "msg"):
        val = log.get(key)
        if isinstance(val, str):
            return val
    
    # Check userData field which contains JSON with message
    user_data = log.get("userData")
    if user_data and isinstance(user_data, str):
        try:
            user_data_obj = json.loads(user_data)
            if isinstance(user_data_obj, dict):
                message = user_data_obj.get("message")
                if isinstance(message, str):
                    return message
        except json.JSONDecodeError:
            pass
    # Sometimes message is nested
    # Try to join stringy values to search
    flattened = []
    def _collect_strings(node: Any):
        if isinstance(node, str):
            flattened.append(node)
        elif isinstance(node, dict):
            for v in node.values():
                _collect_strings(v)
        elif isinstance(node, list):
            for v in node:
                _collect_strings(v)
    _collect_strings(log)
    if flattened:
        # Prefer lines containing 'enrichment object'
        for s in flattened:
            if "enrichment object" in s:
                return s
        return flattened[0]
    return None


def extract_seqnos_from_logs(logs: List[Dict[str, Any]]) -> List[int]:
    seqnos: List[int] = []
    for lg in logs:
        msg = extract_message_field(lg)
        parsed = extract_json_after_label_from_text(msg or "")
        if not parsed:
            # Some sources put JSON directly in message
            try:
                parsed = json.loads(msg) if msg else None
            except Exception:
                parsed = None
        if not parsed:
            continue
        # Expected: {"enrichTransaction":{"data":{"seqno":280141,...}}}
        seq = deep_get(parsed, ["enrichTransaction", "data", "seqno"])
        if isinstance(seq, int):
            seqnos.append(seq)
        else:
            # Sometimes seqno can be string
            if isinstance(seq, str) and seq.isdigit():
                seqnos.append(int(seq))
    # de-duplicate, keep order
    seen = set()
    uniq: List[int] = []
    for s in seqnos:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def extract_pairs_seqno_txid(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pairs: List[Dict[str, Any]] = []
    for lg in logs:
        msg = extract_message_field(lg)
        parsed = extract_json_after_label_from_text(msg or "")
        if not parsed:
            try:
                parsed = json.loads(msg) if msg else None
            except Exception:
                parsed = None
        if not parsed:
            continue
        seq = deep_get(parsed, ["enrichTransaction", "data", "seqno"])
        if isinstance(seq, str) and seq.isdigit():
            seq = int(seq)
        if not isinstance(seq, int):
            continue
        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
            except json.JSONDecodeError:
                pass
        
        # If not found in userData, try other locations
        if txid is None:
            txid = deep_get(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            txid = find_key_recursive(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            # sometimes it's flattened as "metadata.requestContext.txId"
            txid = lg.get("metadata.requestContext.txId")
        if txid is None:
            # As last resort, scan strings that look like 'metadata.requestContext.txId:<uuid>'
            text = json.dumps(lg, ensure_ascii=False)
            m = re.search(r"metadata\.requestContext\.txId[\"']?\s*[:=]\s*[\"']?([0-9a-fA-F-]{20,})", text)
            txid = m.group(1) if m else None
        if txid is None:
            continue
        pairs.append({"Seqno": seq, "metadata.requestContext.txId": str(txid)})
    return pairs


def extract_completed_txids(logs: List[Dict[str, Any]]) -> List[str]:
    """Extract transaction IDs that have COMPLETED status from logs."""
    completed_txids = []
    for lg in logs:
        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
            except json.JSONDecodeError:
                pass
        
        # If not found in userData, try other locations
        if txid is None:
            txid = deep_get(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            txid = find_key_recursive(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            # sometimes it's flattened as "metadata.requestContext.txId"
            txid = lg.get("metadata.requestContext.txId")
        if txid is None:
            # As last resort, scan strings that look like 'metadata.requestContext.txId:<uuid>'
            text = json.dumps(lg, ensure_ascii=False)
            m = re.search(r"metadata\.requestContext\.txId[\"']?\s*[:=]\s*[\"']?([0-9a-fA-F-]{20,})", text)
            txid = m.group(1) if m else None
        
        if txid:
            completed_txids.append(str(txid))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_txids = []
    for txid in completed_txids:
        if txid not in seen:
            seen.add(txid)
            unique_txids.append(txid)
    
    return unique_txids


def extract_source_ids(logs: List[Dict[str, Any]]) -> Dict[str, str]:
    """Extract sourceId for each transaction ID from logs."""
    txid_to_sourceid = {}
    
    for lg in logs:
        # Try to get txId from userData first
        txid = None
        user_data = lg.get("userData")
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                txid = deep_get(user_data_obj, ["metadata", "requestContext", "txId"])
            except json.JSONDecodeError:
                pass
        
        # If not found in userData, try other locations
        if txid is None:
            txid = deep_get(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            txid = find_key_recursive(lg, ["metadata", "requestContext", "txId"])
        if txid is None:
            # sometimes it's flattened as "metadata.requestContext.txId"
            txid = lg.get("metadata.requestContext.txId")
        if txid is None:
            # As last resort, scan strings that look like 'metadata.requestContext.txId:<uuid>'
            text = json.dumps(lg, ensure_ascii=False)
            m = re.search(r"metadata\.requestContext\.txId[\"']?\s*[:=]\s*[\"']?([0-9a-fA-F-]{20,})", text)
            txid = m.group(1) if m else None
        
        if not txid:
            continue
            
        # Try to get sourceId from various locations
        sourceid = None
        
        # Check userData first for metadata.transaction.sourceId
        if user_data and isinstance(user_data, str):
            try:
                user_data_obj = json.loads(user_data)
                sourceid = deep_get(user_data_obj, ["metadata", "transaction", "sourceId"])
            except json.JSONDecodeError:
                pass
        
        # If not found in userData, try other locations
        if sourceid is None:
            sourceid = deep_get(lg, ["metadata", "transaction", "sourceId"])
        if sourceid is None:
            sourceid = find_key_recursive(lg, ["metadata", "transaction", "sourceId"])
        if sourceid is None:
            # sometimes it's flattened as "metadata.transaction.sourceId"
            sourceid = lg.get("metadata.transaction.sourceId")
        
        # If still not found, try to extract from message text patterns
        if sourceid is None:
            # Look for patterns like "SourceId: 455647" in the message
            message = extract_message_field(lg)
            if message:
                # Pattern 1: "SourceId: 455647"
                m = re.search(r"SourceId:\s*(\d+)", message)
                if m:
                    sourceid = m.group(1)
                else:
                    # Pattern 2: "sourceId":"455647"
                    m = re.search(r"sourceId[\"']?\s*[:=]\s*[\"']?(\d+)", message)
                    if m:
                        sourceid = m.group(1)
        
        # As last resort, scan all text for sourceId patterns
        if sourceid is None:
            text = json.dumps(lg, ensure_ascii=False)
            # Pattern 1: "sourceId":"455647"
            m = re.search(r"sourceId[\"']?\s*[:=]\s*[\"']?(\d+)", text)
            if m:
                sourceid = m.group(1)
            else:
                # Pattern 2: "SourceId: 455647"
                m = re.search(r"SourceId:\s*(\d+)", text)
                if m:
                    sourceid = m.group(1)
        
        if sourceid:
            txid_to_sourceid[str(txid)] = str(sourceid)
    
    return txid_to_sourceid


def prompt_inputs() -> Dict[str, Any]:
    print("Enter transaction IDs (comma or space separated):")
    tx_raw = input("> ").strip()
    # split by comma or whitespace
    tx_ids = [t for t in re.split(r"[,\s]+", tx_raw) if t]

    print("Enter tenant id:")
    tenant_id = input("> ").strip()

    print("Enter 'after' timestamp (e.g., 'Sep 7, 2025, 4:15:50 PM', '7 Sept 2025, 16:15:59', or '2025-01-20T00:00:00Z'):")
    after_raw = input("> ").strip()
    print("Enter 'before' timestamp (e.g., 'Sep 7, 2025, 7:15:50 PM', '7 Sept 2025, 19:15:59', or '2025-01-20T00:00:00Z'):")
    before_raw = input("> ").strip()

    after_date = parse_utc_timestamp(after_raw, is_after=True)
    before_date = parse_utc_timestamp(before_raw, is_after=False)
    
    # Validate that after is earlier than before
    after_dt = datetime.fromisoformat(after_date[:-1] + '+00:00')
    before_dt = datetime.fromisoformat(before_date[:-1] + '+00:00')
    if after_dt >= before_dt:
        raise ValueError("'after' must be earlier than 'before'.")

    return {
        "tx_ids": tx_ids,
        "tenant_id": tenant_id,
        "after_date": after_date,
        "before_date": before_date,
    }


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        print("No rows to write. Skipping CSV.")
        return
    fieldnames = ["Seqno", "metadata.requestContext.txId", "sourceId", "Status"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({
                "Seqno": r["Seqno"], 
                "metadata.requestContext.txId": r["metadata.requestContext.txId"],
                "sourceId": r.get("sourceId", "Unknown"),
                "Status": r.get("Status", "Unknown")
            })
    print(f"Wrote {len(rows)} rows to {path}")


def update_csv_with_status(csv_path: str, completed_txids: List[str], sourceid_mapping: Dict[str, str]) -> None:
    """Update CSV file with completion status and mark other txIds as 'Safe to fail'."""
    if not completed_txids:
        print("No completed transaction IDs found. CSV will remain unchanged.")
        return
    
    # Read existing CSV
    rows = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # Add sourceId to each row
    for row in rows:
        txid = row["metadata.requestContext.txId"]
        row["sourceId"] = sourceid_mapping.get(txid, "Unknown")
    
    # Create a set of completed txIds for faster lookup
    completed_set = set(completed_txids)
    
    # Group rows by (seqno, sourceId) to find txIds with same seqno AND sourceId
    seqno_sourceid_groups = {}
    for row in rows:
        seqno = int(row["Seqno"])
        sourceid = row["sourceId"]
        key = (seqno, sourceid)
        if key not in seqno_sourceid_groups:
            seqno_sourceid_groups[key] = []
        seqno_sourceid_groups[key].append(row)
    
    # Update status for each row
    for row in rows:
        txid = row["metadata.requestContext.txId"]
        seqno = int(row["Seqno"])
        sourceid = row["sourceId"]
        
        if txid in completed_set:
            row["Status"] = "Completed"
        else:
            # Check if any txId with the same seqno AND sourceId is completed
            same_group_rows = seqno_sourceid_groups.get((seqno, sourceid), [])
            has_completed_sibling = any(
                other_row["metadata.requestContext.txId"] in completed_set 
                for other_row in same_group_rows 
                if other_row["metadata.requestContext.txId"] != txid
            )
            
            if has_completed_sibling:
                row["Status"] = "Safe to fail"
            else:
                row["Status"] = "Unknown"
    
    # Write updated CSV
    fieldnames = ["Seqno", "metadata.requestContext.txId", "sourceId", "Status"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    
    print(f"Updated CSV with status information:")
    print(f"  - Completed: {len([r for r in rows if r['Status'] == 'Completed'])}")
    print(f"  - Safe to fail: {len([r for r in rows if r['Status'] == 'Safe to fail'])}")
    print(f"  - Unknown: {len([r for r in rows if r['Status'] == 'Unknown'])}")


def main():
    ensure_api_key()
    try:
        params = prompt_inputs()
    except Exception as e:
        print(f"Input error: {e}", file=sys.stderr)
        sys.exit(2)

    tx_ids: List[str] = params["tx_ids"]
    tenant_id: str = params["tenant_id"]
    start_date: str = params["after_date"]
    end_date: str = params["before_date"]

    # Step 2: Build first query
    q1 = build_first_query(tx_ids)
    print("\nRunning first query (seqno discovery)...")
    # Step 3: Query Coralogix DataPrime
    try:
        resp1 = request_dataprime(q1, start_date, end_date)
    except requests.HTTPError as e:
        print(f"HTTP error from DataPrime: {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"Request error: {e}", file=sys.stderr)
        sys.exit(3)

    logs1 = extract_logs_from_response(resp1)
    print(f"Fetched {len(logs1)} logs from first query.")

    # Step 4: Extract seqnos
    seqnos = extract_seqnos_from_logs(logs1)
    print(f"Discovered {len(seqnos)} unique seqno values.")

    if not seqnos:
        print("No seqno values found. Exiting.")
        sys.exit(0)

    # Step 5: Build second query
    q2 = build_second_query(tenant_id, seqnos)
    print("\nRunning second query (tenant + seqno)...")

    # Step 6: Query again
    try:
        resp2 = request_dataprime(q2, start_date, end_date)
    except requests.HTTPError as e:
        print(f"HTTP error from DataPrime: {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
        sys.exit(4)
    except Exception as e:
        print(f"Request error: {e}", file=sys.stderr)
        sys.exit(4)

    logs2 = extract_logs_from_response(resp2)
    print(f"Fetched {len(logs2)} logs from second query.")

    # Step 7: Extract pairs
    pairs = extract_pairs_seqno_txid(logs2)
    print(f"Extracted {len(pairs)} seqno/txId pairs.")

    # Step 8: Write initial CSV
    write_csv(pairs, "seqno_txid.csv")

    # Step 9: Third query - Check completion status
    if pairs:
        # Extract all txIds from pairs for the third query
        all_txids = [pair["metadata.requestContext.txId"] for pair in pairs]
        
        q3 = build_third_query(all_txids)
        print("\nRunning third query (completion status check)...")
        print(f"Third query: {q3}")
        print("Note: If no logs are found, the query format might need adjustment based on your Coralogix data structure.")
        
        try:
            resp3 = request_dataprime(q3, start_date, end_date)
        except requests.HTTPError as e:
            print(f"HTTP error from DataPrime (third query): {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
            print("Continuing without completion status check...")
        except Exception as e:
            print(f"Request error (third query): {e}", file=sys.stderr)
            print("Continuing without completion status check...")
        else:
            logs3 = extract_logs_from_response(resp3)
            print(f"Fetched {len(logs3)} logs from third query.")
            
            if logs3:
                print(f"Sample log from third query: {logs3[0]}")
            
            # Extract completed txIds
            completed_txids = extract_completed_txids(logs3)
            print(f"Found {len(completed_txids)} completed transaction IDs.")
            
            # Step 10: Fourth query - Extract sourceId information
            q4 = build_fourth_query(all_txids)
            print("\nRunning fourth query (sourceId extraction)...")
            print(f"Fourth query: {q4}")
            
            try:
                resp4 = request_dataprime(q4, start_date, end_date)
            except requests.HTTPError as e:
                print(f"HTTP error from DataPrime (fourth query): {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
                print("Continuing without sourceId extraction...")
                sourceid_mapping = {}
            except Exception as e:
                print(f"Request error (fourth query): {e}", file=sys.stderr)
                print("Continuing without sourceId extraction...")
                sourceid_mapping = {}
            else:
                logs4 = extract_logs_from_response(resp4)
                print(f"Fetched {len(logs4)} logs from fourth query.")
                
                if logs4:
                    print(f"Sample log from fourth query: {logs4[0]}")
                
                # Extract sourceId mapping
                sourceid_mapping = extract_source_ids(logs4)
                print(f"Found sourceId for {len(sourceid_mapping)} transaction IDs.")
            
            # Update CSV with status information (including sourceId logic)
            update_csv_with_status("seqno_txid.csv", completed_txids, sourceid_mapping)


if __name__ == "__main__":
    main()