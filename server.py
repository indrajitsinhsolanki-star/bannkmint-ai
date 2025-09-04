from fastapi import FastAPI, APIRouter, UploadFile, File, HTTPException, Depends, Query, Header, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import os
import logging
import hashlib
import pandas as pd
import io
from pathlib import Path
from pydantic import BaseModel, Field, validator
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
from dateutil import parser as date_parser

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Rate limiter setup
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="BannkMint AI", description="CSV Transaction Processing API", version="1.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Models
class Transaction(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    date: str  # YYYY-MM-DD format
    description: str
    amount: float
    currency: str = "USD"
    balance: Optional[float] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    hash_key: str  # For deduplication

class TransactionResponse(BaseModel):
    id: str
    date: str
    description: str
    amount: float
    currency: str
    balance: Optional[float]
    created_at: datetime

class UploadResponse(BaseModel):
    imported: int
    skipped: int

class TransactionsResponse(BaseModel):
    data: List[TransactionResponse]
    page: int
    limit: int
    total: int

class HealthResponse(BaseModel):
    status: str

# API Key Authentication
async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != "dev-key":
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key

# Utility functions
def normalize_date(date_str: str) -> str:
    """Convert date string to YYYY-MM-DD format"""
    try:
        # Try parsing common formats
        if '/' in date_str:
            # Handle DD/MM/YYYY or MM/DD/YYYY
            parts = date_str.split('/')
            if len(parts) == 3:
                if len(parts[2]) == 4:  # DD/MM/YYYY
                    day, month, year = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        elif '-' in date_str:
            # Handle YYYY-MM-DD
            date_obj = date_parser.parse(date_str)
            return date_obj.strftime('%Y-%m-%d')
        
        # Fallback to dateutil parser
        date_obj = date_parser.parse(date_str)
        return date_obj.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Date parsing error for '{date_str}': {e}")
        raise ValueError(f"Invalid date format: {date_str}")

def generate_hash(date: str, description: str, amount: float, currency: str) -> str:
    """Generate hash for deduplication"""
    hash_string = f"{date}|{description}|{amount}|{currency}"
    return hashlib.md5(hash_string.encode()).hexdigest()

def validate_csv_content(df: pd.DataFrame) -> List[str]:
    """Validate CSV content and return list of errors"""
    errors = []
    required_columns = ['date', 'description', 'amount']
    
    # Check required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {', '.join(missing_cols)}")
    
    return errors

# API Routes
@api_router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}

@api_router.post("/uploads/transactions-csv", response_model=UploadResponse)
@limiter.limit("60/minute")
async def upload_transactions_csv(
    request: Request,
    file: UploadFile = File(...),
    api_key: str = Depends(verify_api_key)
):
    """Upload and process transactions CSV file"""
    
    # Validate file type
    if not file.content_type or 'csv' not in file.content_type.lower():
        raise HTTPException(status_code=415, detail="Only CSV files are supported")
    
    # Validate file size (10MB limit)
    if file.size and file.size > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File size exceeds 10MB limit")
    
    try:
        # Read and parse CSV
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        
        # Validate CSV structure
        validation_errors = validate_csv_content(df)
        if validation_errors:
            raise HTTPException(status_code=422, detail={"errors": validation_errors})
        
        imported = 0
        skipped = 0
        row_errors = []
        
        for index, row in df.iterrows():
            try:
                # Extract and validate data
                date_str = str(row['date']).strip()
                description = str(row['description']).strip()
                amount = float(row['amount'])
                currency = str(row.get('currency', 'USD')).strip() or 'USD'
                balance = float(row['balance']) if pd.notna(row.get('balance')) else None
                
                # Normalize date
                normalized_date = normalize_date(date_str)
                
                # Generate hash for deduplication
                hash_key = generate_hash(normalized_date, description, amount, currency)
                
                # Check if transaction already exists
                existing = await db.transactions.find_one({"hash_key": hash_key})
                if existing:
                    skipped += 1
                    continue
                
                # Create transaction object
                transaction = Transaction(
                    date=normalized_date,
                    description=description,
                    amount=amount,
                    currency=currency,
                    balance=balance,
                    hash_key=hash_key
                )
                
                # Insert into database
                await db.transactions.insert_one(transaction.dict())
                imported += 1
                
            except Exception as e:
                row_errors.append(f"Row {index + 2}: {str(e)}")
        
        # If there are row errors, return 422
        if row_errors:
            raise HTTPException(status_code=422, detail={"errors": row_errors})
        
        logger.info(f"CSV processed: {imported} imported, {skipped} skipped")
        return {"imported": imported, "skipped": skipped}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV processing error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")

@api_router.get("/transactions", response_model=TransactionsResponse)
async def get_transactions(
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=200, description="Items per page")
):
    """Get transactions with optional date filtering and pagination"""
    
    try:
        # Build query filters
        query = {}
        
        # Default to last 30 days if no date range provided
        if not from_date and not to_date:
            thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
            query["date"] = {"$gte": thirty_days_ago}
        else:
            date_filter = {}
            if from_date:
                date_filter["$gte"] = from_date
            if to_date:
                date_filter["$lte"] = to_date
            if date_filter:
                query["date"] = date_filter
        
        # Get total count
        total = await db.transactions.count_documents(query)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch transactions (sorted by date desc)
        cursor = db.transactions.find(query).sort("date", -1).skip(skip).limit(limit)
        transactions = await cursor.to_list(length=limit)
        
        # Convert to response format
        transaction_responses = []
        for txn in transactions:
            transaction_responses.append(TransactionResponse(
                id=txn["id"],
                date=txn["date"],
                description=txn["description"],
                amount=txn["amount"],
                currency=txn["currency"],
                balance=txn.get("balance"),
                created_at=txn["created_at"]
            ))
        
        return {
            "data": transaction_responses,
            "page": page,
            "limit": limit,
            "total": total
        }
        
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()