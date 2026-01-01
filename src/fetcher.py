import requests
import json
import time
from typing import List, Dict, Any, Optional

class YargitayFetcher:
    def __init__(self):
        self.api_url = "https://karararama.yargitay.gov.tr/aramadetaylist"
        self.headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://karararama.yargitay.gov.tr/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def search(self, 
               query: str, 
               limit: int = 10, 
               page: int = 1,
               daire: str = "", # Default to empty string for ALL
               year: str = "",
               start_date: str = "",
               end_date: str = "") -> List[Dict[str, Any]]:
        """
        Fetches decisions from Yargitay API.
        """
        # Hack for testing year: if query is digit, treat as year
        karar_yil = year
        if query.isdigit() and len(query) == 4:
            karar_yil = query
            query = "" # Clear keyword if searching by year

        # Handle explicit 'ALL' passed from somewhere else
        if daire == "ALL":
            daire = ""

        payload = {
            "data": {
                "arananKelime": query,
                "birimYrgKurulDaire": daire,
                "kararYil": karar_yil,
                "baslangicTarihi": start_date,
                "bitisTarihi": end_date,
                "pageSize": limit,
                "pageNumber": page,
                "esasYil": "", "esasIlkSiraNo": "", "esasSonSiraNo": "",
                "kararIlkSiraNo": "", "kararSonSiraNo": "",
            }
        }

        try:
            # print(f"Fetching page {page} for query '{query}' (Year: {karar_yil})...")
            response = requests.post(
                self.api_url, 
                data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                headers=self.headers, 
                verify=False,
                timeout=30
            )

            if response.status_code != 200:
                print(f"Error: API returned status code {response.status_code}")
                return []

            data = response.json()
            # Debug Payload
            # print("DEBUG REQUEST PAYLOAD:", json.dumps(payload, indent=2, ensure_ascii=False))
            # Debug
            # print("DEBUG API RESPONSE:", json.dumps(data, indent=2, ensure_ascii=False))
            
            if data and data.get('data') and 'data' in data['data']:
                return data['data']['data']
            
            print("Warning: API returned unexpected structure.")
            if data: print("Keys:", data.keys())
            return []

        except Exception as e:
            print(f"Error during API request: {e}")
            return []

    def get_decision_text(self, decision_id: str) -> str:
        """
        Fetches the full text of a decision using its ID.
        URL: https://karararama.yargitay.gov.tr/getDokuman?id={id}
        """
        url = f"https://karararama.yargitay.gov.tr/getDokuman?id={decision_id}"
        try:
            # Random sleep to avoid being blocked
            time.sleep(0.3)
            
            response = requests.get(
                url, 
                headers=self.headers, 
                verify=False, 
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"Error fetching text for {decision_id}: Status {response.status_code}")
                return "Full text download failed."
                
            # Response is XML/HTML: <AdaletResponseDto><data><html>...</html></data></AdaletResponseDto>
            # We need to extract the content inside <html>...</html> and strip tags.
            content = response.text
            
            # Simple Regex cleanup
            # 1. Extract body/html part roughly
            import re
            clean_text = re.sub(r'<[^>]+>', ' ', content) # Strip all tags
            clean_text = clean_text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>')
            clean_text = " ".join(clean_text.split()) # Normalize whitespace
            
            # Remove the wrapping DTO junk if it persists
            clean_text = clean_text.replace("AdaletResponseDto", "").replace("data", "")
            
            return clean_text.strip()
            
        except Exception as e:
            print(f"Exception fetching text for {decision_id}: {e}")
            return f"Error downloading text: {e}"

if __name__ == "__main__":
    # Test
    fetcher = YargitayFetcher()
    results = fetcher.search(query="boşanma", limit=5)
    print(f"Found {len(results)} results.")
    if results:
        print("Sample:", results[0].get('daire', 'N/A'))
