import re
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from .config import Config

# Import all handlers
from .database import DatabaseHandler
from .hf_storage import HFStorageHandler
from .local_database import LocalDatabaseHandler
from .local_storage import LocalStorageHandler

class Uploader:
    def __init__(self):
        Config.validate()
        
        if Config.STORAGE_MODE == 'local':
            print("--- RUNNING IN LOCAL MODE ---")
            self.db = LocalDatabaseHandler()
            self.storage = LocalStorageHandler()
        else:
            print("--- RUNNING IN REMOTE (CLOUD) MODE ---")
            self.db = DatabaseHandler()
            self.storage = HFStorageHandler()
            
        self.BATCH_SIZE = 100

    def extract_metadata(self, text: str) -> Dict[str, Any]:
        """
        Extracts metadata (Daire, Esas, Karar, Tarih) from the raw text using Regex.
        
        Pattern Analysis based on samples:
        - "14. Hukuk Dairesi  2011/2628  E., 2011/3698  K."
        - "15. Ceza Dairesi         2014/4557 E.   ,   2015/22056 K."
        - "23. Hukuk Dairesi        2015/3617 E.   ,   2017/3781 K."
        
        Date Extraction:
        - "23.03.2011 tarihinde oybirliği ile"
        - "16.05.2018 tarihinde oybirliğiyle"
        """
        metadata = {
            "daire": None,
            "esas_no": None,
            "karar_no": None,
            "karar_tarihi": None
        }
        
        # Normalize text slightly for regex
        clean_text = text.replace('\xa0', ' ').replace('&nbsp;', ' ')
        
        # Strip simple HTML tags if present (since `icerik_ham` might contain them or `icerik_formatli` is passed)
        clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
        
        # Regex for Header Line (Daire, Esas, Karar)
        # 14. Hukuk Dairesi ... 2011/2628 E. ... 2011/3698 K.
        # Capture Group 1: Daire Name
        # Capture Group 2: Esas No
        # Capture Group 3: Karar No
        header_pattern = r"([0-9]+\.\s+[a-zA-Z\s]+Dairesi).*?(\d{4}\/\d+)\s*E\..*?(\d{4}\/\d+)\s*K\."
        
        header_match = re.search(header_pattern, clean_text, re.DOTALL | re.IGNORECASE)
        if header_match:
            metadata["daire"] = header_match.group(1).strip()
            metadata["esas_no"] = header_match.group(2).strip()
            metadata["karar_no"] = header_match.group(3).strip()
            
        # Regex for Date (Tarih)
        # Finds DD.MM.YYYY format usually near "oybirliği" or "karar verildi"
        date_pattern = r"(\d{2}\.\d{2}\.\d{4})\s+tarihinde"
        date_match = re.search(date_pattern, clean_text)
        if date_match:
            try:
                date_str = date_match.group(1)
                # Convert to YYYY-MM-DD for SQL Date
                dt_obj = datetime.strptime(date_str, "%d.%m.%Y")
                metadata["karar_tarihi"] = dt_obj.strftime("%Y-%m-%d")
            except ValueError:
                pass
                
        return metadata

    def process_data(self, data: List[Dict]):
        """
        Processes a list of records (dicts) in batches.
        """
        print(f"Total records found: {len(data)}")
        
        batch = []
        for i, record in enumerate(data):
            # Extract ID (support both 'id' and 'Id')
            record_id = record.get("id") or record.get("Id")
            if not record_id:
                continue

            extracted = {}
            summary = ""
            storage_object = {}

            # Distinguish between our API format and legacy format
            if "icerik_ham" in record and "daire" in record and "esasNo" in record:
                # API Format or previously enriched format
                content = record.get('icerik_ham', '')
                
                # Use metadata from record directly
                extracted = {
                    "daire": record.get("daire"),
                    "esas_no": record.get("esasNo"),
                    "karar_no": record.get("kararNo"),
                    "karar_tarihi": record.get("kararTarihi")
                }
                summary = record.get('ai_ozet', '')
                
                storage_object = {
                    "id": str(record.get("id")),
                    "daire": record.get("daire"),
                    "esasNo": record.get("esasNo"),
                    "kararNo": record.get("kararNo"),
                    "kararTarihi": record.get("kararTarihi"),
                    "icerik_ham": content,
                    "ai_ozet": summary
                }
                # Add optional fields
                if "arananKelime" in record:
                    storage_object["arananKelime"] = record["arananKelime"]

            else:
                # Raw legacy extraction logic (fallback)
                content = record.get('icerik_ham', '')
                if not content: continue
                
                extracted = self.extract_metadata(content)
                summary = record.get('ai_ozet') or content[:200]
                
                storage_object = {
                    "id": str(record.get("id")), 
                    "daire": extracted.get("daire"),
                    "esasNo": extracted.get("esas_no"),
                    "kararNo": extracted.get("karar_no"),
                    "kararTarihi": extracted.get("karar_tarihi"),
                    "icerik_ham": content,
                    "ai_ozet": summary
                }

            # Truncate summary
            if summary and len(summary) > 250:
                summary = summary[:250] + "..."

            batch.append({
                "storage_object": storage_object,
                "metadata": {
                    "id": int(record_id) if str(record_id).isdigit() else 0,
                    "daire": extracted.get("daire"),
                    "esas_no": extracted.get("esas_no"),
                    "karar_no": extracted.get("karar_no"),
                    "karar_tarihi": extracted.get("karar_tarihi"),
                    "ozet": summary
                }
            })
            
            if len(batch) >= self.BATCH_SIZE:
                self._upload_and_index(batch)
                batch = []
        
        # Flush remaining
        if batch:
            self._upload_and_index(batch)

    def process_file(self, file_path: str):
        """
        Reads the large SQL/JSON file and processes in batches.
        For simplicity in this skeleton, we assume lines of JSON or a custom parser.
        Since source is SQL Dump, strictly speaking we'd need a robust SQL parser.
        HOWEVER, for 'Raw JSON' scenario mentioned in prompts, we often assume 
        we can convert SQL dump to a list of dicts first or read an intermediate JSON.
        
        Here, I will implement a loader that expects a LIST of JSON objects
        (mimicking what would happen after we convert the SQL dump to JSON).
        """
        print(f"Processing {file_path}...")
        
        # For demonstration, reading the whole file if small, 
        # or line-by-line if JSONL. Let's assume JSONL or list of dicts.
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f) # Assuming list of objects
        except json.JSONDecodeError:
            print("File is not valid JSON. Ensure input is converted from SQL to JSON.")
            return

        total_records = len(data)
        print(f"Total records found: {total_records}")
        
        batch = []
        for i, record in enumerate(data):
            # Extract basic fields
            record_id = record.get('id')
            if not record_id:
                continue

            extracted = {}
            summary = ""
            content = ""
            
            # Check if this is API data (has 'daire', 'esasNo', 'kararNo')
            if "daire" in record and "esasNo" in record:
                # API Data
                extracted["daire"] = record.get("daire")
                extracted["esas_no"] = record.get("esasNo")
                extracted["karar_no"] = record.get("kararNo")
                extracted["karar_tarihi"] = None
                
                # Parse date "28.10.2009" to YYYY-MM-DD
                kt = record.get("kararTarihi")
                if kt:
                    try:
                        parts = kt.split('.')
                        if len(parts) == 3:
                            extracted["karar_tarihi"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
                    except:
                        pass
                
                summary = record.get("daire", "") + " " + record.get("kararNo", "")
                
                # Standardize Storage Object for API Data
                storage_object = {
                    "id": str(record.get("id")), # Force String ID
                    "daire": record.get("daire"),
                    "esasNo": record.get("esasNo"),
                    "kararNo": record.get("kararNo"),
                    "kararTarihi": record.get("kararTarihi"),
                    "icerik_ham": record.get("icerik_ham", "Full text not available (Metadata only download)."),
                    "ai_ozet": record.get("ai_ozet", "")
                }
                
                # Add 'arananKelime' if present, but optional
                if "arananKelime" in record:
                    storage_object["arananKelime"] = record["arananKelime"]

            else:
                # Raw legacy data (SQL dump), use regex extraction
                content = record.get('icerik_ham', '')
                if not content: continue
                
                extracted = self.extract_metadata(content)
                summary = record.get('ai_ozet') or content[:200]
                
                # Standardize Storage Object for Legacy Data
                storage_object = {
                    "id": str(record.get("id")), # Force String ID
                    "daire": extracted.get("daire") or record.get("daire"),
                    "esasNo": extracted.get("esas_no") or record.get("esasNo"),
                    "kararNo": extracted.get("karar_no") or record.get("kararNo"),
                    "kararTarihi": extracted.get("karar_tarihi"),
                    "icerik_ham": content,
                    "ai_ozet": summary
                }

            # Truncate summary for VARCHAR(255)
            if summary and len(summary) > 250:
                summary = summary[:250] + "..."

            batch.append({
                "storage_object": storage_object,
                "metadata": {
                    "id": int(record_id), # Keep Metadata ID as Int for Supabase if column is int4/int8
                    "daire": extracted.get("daire"),
                    "esas_no": extracted.get("esas_no"),
                    "karar_no": extracted.get("karar_no"),
                    "karar_tarihi": extracted.get("karar_tarihi"),
                    "ozet": summary
                    # "full_text_url": TO BE FILLED AFTER UPLOAD
                }
            })
            
            if len(batch) >= self.BATCH_SIZE:
                self._upload_and_index(batch)
                batch = []
        
        # flush remaining
        if batch:
            self._upload_and_index(batch)

    def _upload_and_index(self, batch: List[Dict]):
        # 1. Upload Full Texts to Hugging Face
        # Extract just the storage objects for the JSON file
        storage_data = [item["storage_object"] for item in batch]
        
        print(f"Uploading batch of {len(batch)} records to HF...")
        try:
            raw_url = self.storage.upload_batch(storage_data)
        except Exception as e:
            print(f"Skipping batch due to upload error: {e}")
            return
            
        if not raw_url:
            print("Upload failed, no URL returned.")
            return

        # 2. Update Metadata with URL and Insert to Supabase
        metadata_list = []
        for item in batch:
            meta = item["metadata"]
            meta["full_text_url"] = raw_url # All items in this batch share the same file URL
            metadata_list.append(meta)
            
        print("Indexing metadata in Supabase...")
        self.db.upsert_metadata(metadata_list)
        print("Batch complete.")

if __name__ == "__main__":
    import sys
    import glob
    import os
    import argparse
    import time
    from .fetcher import YargitayFetcher

    # Parse arguments
    parser = argparse.ArgumentParser(description="Yargitay Decision Uploader")
    parser.add_argument("file", nargs="?", help="Path to JSON file to process")
    parser.add_argument("--fetch", type=str, nargs='+', help="Search query (or queries) to fetch from API")
    parser.add_argument("--limit", type=int, default=100, help="Number of records to fetch")
    
    args = parser.parse_args()
    
    uploader = Uploader()
    
    # 1. Fetch Mode
    if args.fetch:
        fetcher = YargitayFetcher()
        # Support multiple keywords: 
        # 1. As separate args: --fetch "dava" "boşanma" -> ['dava', 'boşanma']
        # 2. As comma strings: --fetch "dava,boşanma" -> ['dava,boşanma']
        # Combine and flatten:
        raw_keywords = args.fetch # This is a list
        keywords = []
        for k in raw_keywords:
            # Split by comma if present
            keywords.extend([sub.strip() for sub in k.split(',') if sub.strip()])
            
        total_global_fetched = 0
        
        for query in keywords:
            print(f"\n{'='*40}")
            print(f"Starting fetch loop for: '{query}'")
            print(f"{'='*40}")
            
            page = 1
            page_size = 50 
            fetched_for_query = 0
            
            # Run until limit or no more results
            while fetched_for_query < args.limit:
                remaining = args.limit - fetched_for_query
                current_limit = min(page_size, remaining)
                
                print(f"Fetching page {page} for '{query}'...")
                
                # Fetch
                try:
                    batch = fetcher.search(query=query, limit=page_size, page=page)
                except Exception as e:
                    print(f"Search failed: {e}")
                    time.sleep(10)
                    continue

                if not batch:
                    print(f"No more results for '{query}'.")
                    break
                
                # Enrich batch with full text
                print(f"Fetching full text for {len(batch)} records...")
                enriched_batch = []
                for i, record in enumerate(batch):
                    rec_id = record.get('id')
                    if rec_id:
                        print(f"[{i+1}/{len(batch)}] Downloading content for ID {rec_id}...", end='\r')
                        full_text = fetcher.get_decision_text(str(rec_id))
                        record['icerik_ham'] = full_text
                        enriched_batch.append(record)
                print("") # newline
                    
                # Process and Upload Batch immediately
                if enriched_batch:
                    uploader.process_data(enriched_batch)
                
                fetched_count = len(batch)
                fetched_for_query += fetched_count
                total_global_fetched += fetched_count
                page += 1
                
                # Add a delay between pages to prevent banning
                time.sleep(2)
                
        print(f"\nGlobal processing complete. Total records: {total_global_fetched}")
        # Processing complete
        pass

    # 2. File Mode (Command Line Arg)
    elif args.file:
        if os.path.exists(args.file):
             uploader.process_file(args.file)
        else:
            print(f"File not found: {args.file}")
            
    # 3. Auto-detect downloaded/source
    elif glob.glob("downloaded_data/*.json"):
        files = glob.glob("downloaded_data/*.json")
        print(f"Found {len(files)} downloaded files. Processing most recent...")
        latest_file = max(files, key=os.path.getctime)
        uploader.process_file(latest_file)
        
    elif os.path.exists("source_data.json"):
        print("Processing default source_data.json")
        uploader.process_file("source_data.json")
    else:
        print("No input provided. Use --fetch 'query' or provide a file path.")
