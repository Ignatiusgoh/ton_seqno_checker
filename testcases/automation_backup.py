#!/usr/bin/env python3

import os
import re
import sys
import json
import time
import csv
from datetime import datetime, timezone
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


def parse_utc_timestamp(value: str) -> str:
    s = value.strip()
    
    # Handle UTC timestamp format: 2025-01-20T00:00:00Z
    if s.endswith('Z'):
        try:
            # Validate the UTC timestamp format
            dt = datetime.fromisoformat(s[:-1] + '+00:00')
            # Return the original string if valid
            return s
        except ValueError:
            pass
    
    # Try other ISO-8601 variants and convert to UTC Z format
    s_with_tz = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s_with_tz)
        # Convert to UTC Z format
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    
    # Try a common format "YYYY-MM-DD HH:MM:SS" and assume UTC
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    
    # If all parsing attempts fail, provide helpful error message
    raise ValueError(f"Unrecognized time format: {value}. Expected format: 2025-01-20T00:00:00Z")


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
    
    # Try to parse JSON with better error handling
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        print(f"JSON Parse Error: {e}")
        print(f"Full response text: {resp.text}")
        print("This usually means the API returned an error page instead of JSON")
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
        # metadata.requestContext.txId from log object
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


def prompt_inputs() -> Dict[str, Any]:
    print("Enter transaction IDs (comma or space separated):")
    tx_raw = input("> ").strip()
    # split by comma or whitespace
    tx_ids = [t for t in re.split(r"[,\s]+", tx_raw) if t]

    print("Enter tenant id:")
    tenant_id = input("> ").strip()

    print("Enter 'after' timestamp (UTC format: 2025-01-20T00:00:00Z):")
    after_raw = input("> ").strip()
    print("Enter 'before' timestamp (UTC format: 2025-01-20T00:00:00Z):")
    before_raw = input("> ").strip()

    after_date = parse_utc_timestamp(after_raw)
    before_date = parse_utc_timestamp(before_raw)
    
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
    fieldnames = ["Seqno", "metadata.requestContext.txId"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({"Seqno": r["Seqno"], "metadata.requestContext.txId": r["metadata.requestContext.txId"]})
    print(f"Wrote {len(rows)} rows to {path}")


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

    # Step 8: CSV
    write_csv(pairs, "seqno_txid.csv")


if __name__ == "__main__":
    main()