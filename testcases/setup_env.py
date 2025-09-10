#!/usr/bin/env python3
"""
Helper script to set up environment variables for Coralogix API
"""
import os
import sys
from dotenv import load_dotenv, set_key

# Load existing .env file if it exists
load_dotenv()

def setup_environment():
    print("Coralogix API Setup")
    print("==================")
    print()
    
    # Check if API key is already set
    current_key = os.getenv("CORALOGIX_API_KEY")
    if current_key:
        print(f"Current API key: {current_key[:8]}...{current_key[-4:]}")
        print()
    
    print("Please enter your Coralogix API key:")
    api_key = input("API Key: ").strip()
    
    if not api_key:
        print("No API key provided. Exiting.")
        sys.exit(1)
    
    # Set environment variable for current session
    os.environ["CORALOGIX_API_KEY"] = api_key
    
    # Save to .env file
    set_key('.env', 'CORALOGIX_API_KEY', api_key)
    
    print(f"\nAPI key set for current session: {api_key[:8]}...{api_key[-4:]}")
    print("API key saved to .env file")
    print()
    print("The .env file will be automatically loaded when you run automation.py")
    print()
    
    # Test the API key
    print("Testing API key...")
    try:
        import requests
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "X-API-Key": api_key,
        }
        
        # Simple test query
        payload = {
            "query": "source logs | limit 1",
            "metadata": {
                "startTime": 1609459200000,  # 2021-01-01
                "endTime": 1640995200000,
                "syntax": "QUERY_SYNTAX_LUCENE",
                "limit": 100
                }    # 2022-01-01
        }
        
        resp = requests.post(
            "https://api.coralogix.us/api/v1/dataprime/query",
            headers=headers,
            json=payload,
            timeout=10,
            verify=False
        )
        
        if resp.status_code == 200:
            print("✅ API key is valid!")
        elif resp.status_code == 403:
            print("❌ API key is invalid or has insufficient permissions")
            print("Response:", resp.text)
        else:
            print(f"⚠️  Unexpected response: {resp.status_code}")
            print("Response:", resp.text)
            
    except Exception as e:
        print(f"❌ Error testing API key: {e}")

if __name__ == "__main__":
    setup_environment()
