"""Data fetching module for Google Sheets integration.

Provides functions for fetching data from Google Sheets without any caching.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import logging
import os
from typing import List, Dict, Any
import pandas as pd
import json

# Get the absolute path to the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)

# Set up logging first
logger = logging.getLogger('data_fetch')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(os.path.join(root_dir, 'data_fetch.log'))
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def get_credentials_dict():
    """Get Google Service Account credentials from environment variable or local file."""
    # Try to get credentials from environment variable first (for Render deployment)
    google_credentials = os.getenv('GOOGLE_CREDENTIALS')
    
    if google_credentials:
        try:
            # Parse the JSON string from environment variable
            credentials_dict = json.loads(google_credentials)
            logger.info("Using Google credentials from environment variable")
            return credentials_dict
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse GOOGLE_CREDENTIALS environment variable: {e}")
            raise
    
    # Fall back to local file for development
    local_token_path = os.path.join(root_dir, 'tokens', 'token.json')
    if os.path.exists(local_token_path):
        try:
            with open(local_token_path, 'r') as f:
                credentials_dict = json.load(f)
            logger.info("Using Google credentials from local token file")
            return credentials_dict
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to read local token file: {e}")
            raise
    
    # If neither method works, raise an error
    raise FileNotFoundError("Google credentials not found in environment variable 'GOOGLE_CREDENTIALS' or local token file")


def get_gspread_client() -> gspread.Client:
    """Get an authenticated gspread client."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials_dict = get_credentials_dict()
        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Failed to get gspread client: {e}")
        raise

def get_worksheet(spreadsheet_name: str, worksheet_name: str, row_start: int = 1, column_start: int = 1, limit_columns: int = 0, limit_rows: int = 0) -> pd.DataFrame:
    """
    Retrieves data from a specified worksheet in a Google Spreadsheet, starting from a given row and column,
    and returns the data as a pandas DataFrame.
    
    Args:
        spreadsheet_name (str): The name of the Google Spreadsheet to access.
        worksheet_name (str): The name of the worksheet within the spreadsheet.
        row_start (int): The 1-based index of the row to start reading data (used for headers).
        column_start (int): The 1-based index of the column to start reading data (used for headers).
        limit_columns (int, optional): The maximum number of columns to read from the starting column. If 0, reads all columns.
        limit_rows (int, optional): The maximum number of data rows to read (excluding header row). If 0, reads all rows.
        
    Returns:
        pd.DataFrame: A DataFrame containing the worksheet data with column headers from the specified row.
                      Returns an empty DataFrame if no data is found or an error occurs.
                      Missing values are represented as None instead of empty strings.
                              
    Notes:
        - The function creates a new gspread client for each request.
        - If row_start or column_start is less than 1, they are set to 1.
        - If the starting row or column is beyond the available data, an empty DataFrame is returned.
        - Rows with insufficient columns are padded with None values to match the header length.
        - Empty string values from Google Sheets are converted to None for better pandas handling.
        - The limit_rows parameter applies only to data rows, not including the header row.
        - Errors are logged and result in an empty DataFrame being returned.
    """
    
    start_time = time.time()
    logger.info(f"Starting get_worksheet for {spreadsheet_name}/{worksheet_name} (limit_rows: {limit_rows})")
    
    try:
        # Step 1: Authenticate and create a new gspread client for this request
        client: gspread.Client = get_gspread_client()
        
        # Step 2: Open the specified spreadsheet and worksheet
        sheet = client.open(spreadsheet_name)
        worksheet = sheet.worksheet(worksheet_name)
        
        # Step 3: Retrieve all values from the worksheet
        all_values = worksheet.get_all_values()
        
        # Step 4: Validate that we have data and the starting row is valid
        if not all_values or row_start >= len(all_values):
            logger.warning(f"No data or row_start {row_start} beyond data length {len(all_values)}")
            return pd.DataFrame()
        
        # Step 5: Validate and normalize input parameters
        if row_start < 1:
            row_start = 1
            
        if column_start < 1:
            column_start = 1
        
        # Step 6: Convert to 0-based indexing for array access
        row_idx = row_start - 1
        col_idx = column_start - 1
        
        # Step 7: Extract and validate headers from the starting row and column
        if col_idx >= len(all_values[row_idx]):
            logger.warning(f"Column start {column_start} is beyond the width of row {row_start}")
            return pd.DataFrame()
            
        headers = all_values[row_idx][col_idx:]
        
        # Step 8: Apply column limit to headers if specified
        if limit_columns != 0 and limit_columns < len(headers):
            headers = headers[:limit_columns]

        # Step 9: Process data rows (excluding the header row)
        data_rows = []
        data_rows_processed = 0
        
        # Calculate the range of rows to process
        start_data_row = row_idx + 1
        end_data_row = len(all_values)
        
        # Apply row limit if specified (limit_rows applies to data rows only)
        if limit_rows > 0:
            end_data_row = min(start_data_row + limit_rows, len(all_values))
        
        for data_row_idx in range(start_data_row, end_data_row):
            data_row = all_values[data_row_idx]
            
            # Step 10: Skip rows that are too short (don't reach our starting column)
            if col_idx >= len(data_row):
                continue
                
            # Step 11: Extract the relevant portion of the row based on column start and limit
            row_data = data_row[col_idx:]
            if limit_columns != 0 and limit_columns < len(row_data):
                row_data = row_data[:limit_columns]

            # Step 12: Pad row data with None values if it's shorter than headers
            while len(row_data) < len(headers):
                row_data.append("")
                
            # Step 13: Convert empty strings to None and prepare row for DataFrame
            processed_row = [None if value == "" else value for value in row_data]
            data_rows.append(processed_row)
            data_rows_processed += 1
        
        # Step 14: Create pandas DataFrame from processed data
        df = pd.DataFrame(data_rows, columns=headers)
        
        elapsed = time.time() - start_time
        logger.info(f"Retrieved and processed {data_rows_processed} rows in {elapsed:.2f}s")
        return df
        
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error in get_worksheet after {elapsed:.2f}s: {e}", exc_info=True)
        # Return empty DataFrame if an error occurs
        return pd.DataFrame()
        
    finally:
        # Attempt to clean up the gspread client object
        try:
            del client
        except Exception:
            pass