import requests
import json
import os
import time

class YargitayDownloader:
    def __init__(self):
        self.api_url = "https://karararama.yargitay.gov.tr/aramadetaylist"
        self.output_dir = "downloaded_data"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def search_and_download(self, keyword="", limit=10, page=1):
        """
        Mimics the search.php curl request to fetch real data.
        """
        print(f"Searching for '{keyword}' (Page {page}, Limit {limit})...")
        
        payload = {
            "data": {
                "arananKelime": keyword,
                "birimYrgKurulDaire": "",
                "kararYil": "",
                "baslangicTarihi": "",
                "bitisTarihi": "",
                "pageSize": limit,
                "pageNumber": page,
                "esasYil": "", 
                "esasIlkSiraNo": "", 
                "esasSonSiraNo": "",
                "kararIlkSiraNo": "", 
                "kararSonSiraNo": ""
            }
        }
        
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://karararama.yargitay.gov.tr/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        try:
            # Verify=False to match search.php behavior (CURLOPT_SSL_VERIFYPEER false)
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            # Increased timeout to 60s for heavy queries
            response = requests.post(self.api_url, json=payload, headers=headers, timeout=60, verify=False)
            
            if response.status_code != 200:
                print(f"Error: API returned status code {response.status_code}")
                print(response.text[:200])
                return []

            data = response.json()
            print("FULL RESPONSE:", json.dumps(data, indent=2, ensure_ascii=False))
            
            # Navigate to the actual list of items
            # structure seems to be: { "data": { "data": [ ...list of decisions... ], "recordsFiltered": ... } }
            
            outer_data = data.get("data")
            if outer_data is None:
                print("API returned 'data': null. Result might be empty or error.")
                return []
                
            results = outer_data.get("data", [])
            
            if not results:
                print("No results found in API response (empty list).")
                return []
                
            print(f"Fetched {len(results)} records.")
            
            # Save to file
            timestamp = int(time.time())
            filename = f"yargitay_api_results_{timestamp}_page{page}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
                
            print(f"Saved to {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Exception during request: {e}")
            return []

    def harvest(self, keyword, batch_size=20, max_pages=50):
        """
        Continuously fetches pages until max_pages or no more results.
        """
        page = 1
        total_fetched = 0
        
        print(f"Starting harvest for keyword: '{keyword}'")
        
        while True:
            if max_pages and page > max_pages:
                print(f"Reached max page limit ({max_pages}). Stopping.")
                break
                
            # Fetch current page
            result = self.search_and_download(keyword, limit=batch_size, page=page)
            
            # If result is empty list or None, stop
            if not result:
                print("No more results or error occurred. Stopping.")
                break
                
            # If it returned a filepath, it means success
            
            page += 1
            total_fetched += batch_size # Approx
            time.sleep(1.5) # Polite delay between requests

if __name__ == "__main__":
    downloader = YargitayDownloader()
    
    # query = '"madde" "kanun" "dava" "mahkeme" "karar"' # CAUSES ADALET_RUNTIME_EXCEPTION
    # query = "iş davası tazminat" 
    query = "karar"
    
    # Restore reasonable batch size
    downloader.harvest(keyword=query, batch_size=10, max_pages=1)
