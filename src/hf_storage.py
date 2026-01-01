import json
import uuid
import time
from typing import List, Dict, Any
try:
    from huggingface_hub import HfApi, CommitOperationAdd
except ImportError:
    HfApi = None
    CommitOperationAdd = None

from .config import Config

class HFStorageHandler:
    def __init__(self):
        Config.validate()
        self.api = HfApi(token=Config.HF_TOKEN)
        self.repo_id = Config.HF_REPO_ID
        self.repo_type = "dataset"

    def upload_batch(self, batch_data: List[Dict[str, Any]]) -> str:
        """
        Uploads a batch of decisions as a JSON file to Hugging Face.
        Returns the Raw URL of the uploaded file.
        """
        if not batch_data:
            return ""

        # Create unique filename
        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]
        filename = f"data/batch_{timestamp}_{unique_id}.json"
        
        # Determine the user or organization from repo_id for the URL
        # URL format: https://huggingface.co/datasets/{repo_id}/resolve/main/{filename}
        # Note: We use 'resolve/main' to get the raw pointer, usually redirects to CDN
        raw_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/{filename}"
        
        # Prepare content
        json_content = json.dumps(batch_data, ensure_ascii=False, indent=2)
        json_bytes = json_content.encode('utf-8')

        try:
            # Create a commit operation
            operations = [
                CommitOperationAdd(
                    path_in_repo=filename,
                    path_or_fileobj=json_bytes
                )
            ]
            
            self.api.create_commit(
                repo_id=self.repo_id,
                operations=operations,
                commit_message=f"Upload batch {filename}",
                repo_type=self.repo_type
            )
            
            print(f"Uploaded batch to {filename}")
            return raw_url

        except Exception as e:
            print(f"Error uploading to Hugging Face: {e}")
            raise e

    def fetch_full_text(self, url: str) -> List[Dict[str, Any]]:
        """
        Fetches the content of a batch file from the Raw URL.
        """
        import requests
        try:
            # We need to pass the token if the repo is private
            headers = {"Authorization": f"Bearer {Config.HF_TOKEN}"}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching from {url}: {e}")
            return []
