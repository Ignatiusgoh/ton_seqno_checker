# TON Seqno Automation

This script automates the process of finding sequence numbers (seqno) associated with transaction IDs using the Coralogix DataPrime API.

## Dependencies

The script requires the following Python packages (see `requirements.txt`):
- `requests` - For making HTTP API calls to Coralogix DataPrime
- `python-dotenv` - For loading environment variables from .env file

## Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up your API key** (choose one method):

   **Method 1: Use the setup script (recommended)**:
   ```bash
   python setup_env.py
   ```
   
   **Method 2: Edit the .env file directly**:
   ```bash
   # Edit the .env file and replace 'your_api_key_here' with your actual API key
   nano .env
   ```
   
   **Method 3: Set environment variable manually**:
   ```bash
   export CORALOGIX_API_KEY="your_api_key_here"
   ```

2. **Run the automation script**:
   ```bash
   python automation.py
   ```

## Changes Made

### Fixed Authentication Issues
- Removed hardcoded API key for security
- Added proper environment variable handling
- Added both Bearer token and X-API-Key headers (Coralogix supports both)
- Enabled SSL verification (removed `verify=False`)

### Enhanced Error Handling
- Added detailed 403 Forbidden error messages
- Added debugging output to show request details
- Better error messages for common authentication issues

### Security Improvements
- API key now read from environment variable or .env file
- SSL verification enabled
- Removed hardcoded credentials
- Added .env file support for easier configuration

### Environment File Support
- Added python-dotenv dependency for .env file loading
- Script automatically loads environment variables from .env file
- Added debugging output to show when API key is loaded
- Updated setup script to save API key to .env file

### Timestamp Format Support
- Updated to accept UTC timestamps in format: 2025-01-20T00:00:00Z
- Enhanced timestamp parsing to handle various formats
- Improved error messages for invalid timestamp formats
- Maintains backward compatibility with epoch timestamps

## Troubleshooting

### 403 Forbidden Error
If you still get a 403 error, check:

1. **API Key**: Make sure your API key is valid and not expired
2. **Permissions**: Ensure your API key has DataPrime query permissions
3. **Authentication Method**: The script now tries both Bearer token and X-API-Key headers
4. **Endpoint**: Verify the API endpoint URL is correct

### SSL Warnings
The script now uses `verify=True` for SSL verification. If you get SSL errors, you may need to:
- Update your certificates
- Check your network configuration
- Contact your system administrator

## Usage

1. Run `python setup_env.py` to configure your API key
2. Run `python automation.py` and follow the prompts:
   - Enter transaction IDs (comma or space separated)
   - Enter tenant ID
   - Enter time range (after and before timestamps)
3. The script will output a CSV file with seqno/txId pairs

## Output

The script generates a CSV file named `seqno_txid.csv` with the following columns:
- `Seqno`: The sequence number
- `metadata.requestContext.txId`: The transaction ID
