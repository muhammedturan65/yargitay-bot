import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    # Clean up DATABASE_URL to avoid Copy-Paste errors (spaces, quotes)
    _db_url = os.getenv("DATABASE_URL", "")
    if _db_url:
        _db_url = _db_url.strip().strip("'").strip('"')
    DATABASE_URL = _db_url

    HF_TOKEN = os.getenv("HF_TOKEN")
    HF_REPO_ID = os.getenv("HF_REPO_ID") # username/repo-name
    
    # Options: 'remote' (default) or 'local'
    STORAGE_MODE = os.getenv("STORAGE_MODE", "remote").lower()
    
    # Validation
    @classmethod
    def validate(cls):
        if cls.STORAGE_MODE == 'local':
            return # No API keys needed for local mode
            
        missing = []
        if not cls.HF_TOKEN: missing.append("HF_TOKEN")
        if not cls.HF_REPO_ID: missing.append("HF_REPO_ID")
        
        # Check for DB connection
        if not cls.DATABASE_URL and (not cls.SUPABASE_URL or not cls.SUPABASE_KEY):
            missing.append("DATABASE_URL or (SUPABASE_URL + SUPABASE_KEY)")
        
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

if __name__ == "__main__":
    try:
        Config.validate()
        print("Configuration is valid.")
    except ValueError as e:
        print(f"Configuration Error: {e}")
