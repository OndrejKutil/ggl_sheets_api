from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from data.data_fetch import get_worksheet
import os
from dotenv import load_dotenv
from typing import Optional
import pandas as pd

# Load environment variables
load_dotenv()

app = FastAPI()
security = HTTPBearer()

# API Key authentication
def _verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verifies the API key provided in the Authorization header against the server's configured API key.
    Args:
        credentials (HTTPAuthorizationCredentials): The credentials extracted from the Authorization header.
    Raises:
        HTTPException: If the API key is not configured on the server (500).
        HTTPException: If the provided API key is invalid (401).
    Returns:
        str: The validated API key.
    Example:
        The request should include the following header:
            Authorization: Bearer <API_KEY>
    """
    """Verify the API key from the Authorization header."""
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="API key not configured on server")
    
    if credentials.credentials != api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return credentials.credentials

@app.get('/')
def read_root():
    return {"Hello": "World"}


@app.get('/health')
def read_health():
    return {"status": "healthy"}


@app.get('/data/worksheet')
def get_worksheet_data(
    spreadsheet_name: str = Query(..., description="Name of the Google Spreadsheet"),
    worksheet_name: str = Query(..., description="Name of the worksheet within the spreadsheet"),
    row_start: int = Query(1, description="Starting row for data (1-based index)"),
    column_start: int = Query(1, description="Starting column for data (1-based index)"),
    limit_columns: int = Query(0, description="Maximum number of columns to read (0 = all)"),
    limit_rows: int = Query(0, description="Maximum number of rows to read (0 = all)"),
    api_key: str = Depends(_verify_api_key)
):
    """
    Retrieve data from a specified Google Sheets worksheet.
    
    Parameters can be used to specify the spreadsheet, worksheet, and data range.
    Requires API key authentication via Authorization header.
    """
    try:
        df = get_worksheet(
            spreadsheet_name=spreadsheet_name,
            worksheet_name=worksheet_name,
            row_start=row_start,
            column_start=column_start,
            limit_columns=limit_columns,
            limit_rows=limit_rows
        )
        
        if df.empty:
            return {
                "message": "No data found or unable to retrieve data",
                "data": [],
                "total_rows": 0
            }
        
        # Convert DataFrame to list of dictionaries for JSON response
        data = df.to_dict('records')
        
        return {
            "message": "Data retrieved successfully",
            "total_rows": len(data),
            "columns": list(df.columns),
            "data": data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving worksheet data: {str(e)}")


@app.get('/data/budget_tracker/transactions')
def return_transactions(
    page: Optional[int] = Query(None, ge=1, description="Page number (starting from 1). If not provided, returns all transactions"),
    page_size: Optional[int] = Query(None, ge=1, le=1000, description="Number of transactions per page (max 1000). If not provided, returns all transactions"),
    api_key: str = Depends(_verify_api_key)
):
    """
    Retrieve transactions from the budget tracker with optional pagination.
    
    This endpoint uses fixed spreadsheet and worksheet names for the budget tracker.
    Use page and page_size query parameters for pagination, or omit them to get all transactions.
    Requires API key authentication via Authorization header.
    """
    try:
        # Fixed spreadsheet and worksheet for budget tracker
        spreadsheet_name = "Budget tracker"
        worksheet_name = "transactions"
        
        # Get all data from the worksheet
        df = get_worksheet(
            spreadsheet_name=spreadsheet_name,
            worksheet_name=worksheet_name,
            row_start=1,  # Headers
            column_start=1,
            limit_columns=0,  # Get all columns
            limit_rows=0      # Get all rows
        )
        
        if df.empty:
            return {
                "message": "No transactions found",
                "data": [],
                "total_rows": 0
            }
        
        total_rows = len(df)
        
        # Check if pagination is requested
        if page is not None and page_size is not None:
            # Apply pagination to the DataFrame
            total_pages = (total_rows + page_size - 1) // page_size
            
            skip_rows = (page - 1) * page_size
            start_idx = skip_rows
            end_idx = start_idx + page_size
            paginated_df = df.iloc[start_idx:end_idx]
            
            # Convert to list of dictionaries for JSON response
            data = paginated_df.to_dict('records')
            
            return {
                "message": "Transactions retrieved successfully",
                "data": data,
                "page": page,
                "page_size": page_size,
                "total_rows": total_rows,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        else:
            # Return all transactions without pagination
            data = df.to_dict('records')
            
            return {
                "message": "All transactions retrieved successfully",
                "data": data,
                "total_rows": total_rows
            }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving transactions: {str(e)}")
