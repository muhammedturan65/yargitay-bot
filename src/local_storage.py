import json
import os
import time
import uuid
from typing import List, Dict, Any

class LocalStorageHandler:
    def __init__(self):
        self.storage_dir = "local_storage_data"
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def upload_batch(self, batch_data: List[Dict[str, Any]]) -> str:
        """
        Saves a batch of decisions as a JSON file locally.
        Returns the absolute path (as a URL-like string).
        """
        if not batch_data:
            return ""

        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]
        filename = f"batch_{timestamp}_{unique_id}.json"
        file_path = os.path.join(self.storage_dir, filename)
        
        # Save locally
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(batch_data, f, ensure_ascii=False, indent=2)
            
        print(f"[LOCAL] Saved batch to {file_path}")
        return os.path.abspath(file_path)

    def fetch_full_text(self, url: str) -> List[Dict[str, Any]]:
        """
        Reads the content from the local file path.
        """
        try:
            # url here is actually the file path
            with open(url, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[LOCAL] Error reading from {url}: {e}")
            return []
