# scripts/import_mock_data.py
import os
import json
import sys
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env file")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Table name in Supabase
TABLE_NAME = "staging_raw_prices"

def load_json_file(file_path: str):
    """
    Load JSON data from a file.
    Returns data as list of dicts, or None if error.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data)} records from {file_path}")
        return data
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in {file_path}: {e}")
        return None

def insert_data(data: list, table: str):
    """
    Insert a list of records into the specified Supabase table.
    """
    try:
        response = supabase.table(table).upsert(data, on_conflict='model_name,source,scraped_date').execute()
        print(f"✅ Successfully inserted {len(data)} records into '{table}'.")
        return response
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        return None

def clear_table(table: str):
    """
    Delete all rows from the table (use with caution).
    Assumes an 'id' column > 0.
    """
    try:
        supabase.table(table).delete().gt("id", 0).execute()
        print(f"🗑️ Cleared all data from '{table}'.")
    except Exception as e:
        print(f"❌ Error clearing table: {e}")

if __name__ == "__main__":
    # Default path to mock data file
    DEFAULT_FILE = "data/raw//mock/mock_prices.json"
    
    # Allow passing file path as command-line argument
    file_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    
    # Optional: clear existing data before inserting
    # Uncomment the next line if you want to start fresh
    # clear_table(TABLE_NAME)
    
    # Load data from JSON file
    mock_data = load_json_file(file_path)
    if mock_data is None:
        sys.exit(1)
    
    # Insert data into Supabase
    result = insert_data(mock_data, TABLE_NAME)
    if result:
        print("🎉 Import completed successfully.")
    else:
        print("⚠️ Import failed.")
        sys.exit(1)