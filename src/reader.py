from .database import DatabaseHandler
from .hf_storage import HFStorageHandler
from .local_database import LocalDatabaseHandler
from .local_storage import LocalStorageHandler
from .config import Config
import sys

class Reader:
    def __init__(self):
        Config.validate()
        if Config.STORAGE_MODE == 'local':
            self.db = LocalDatabaseHandler()
            self.storage = LocalStorageHandler()
        else:
            self.db = DatabaseHandler()
            self.storage = HFStorageHandler()

    def search(self, **kwargs):
        results = self.db.search_decisions(**kwargs)
        if not results:
            print("No results found.")
            return []

        print(f"\nFound {len(results)} records:\n")
        print(f"{'ID':<15} | {'Daire':<20} | {'Esas':<12} | {'Karar':<12} | {'Tarih':<12}")
        print("-" * 80)
        
        for r in results:
            d_id = str(r['id'])
            daire = str(r.get('daire') or '')[:20]
            esas = str(r.get('esas_no') or '')
            karar = str(r.get('karar_no') or '')
            tarih = str(r.get('karar_tarihi') or '')
            print(f"{d_id:<15} | {daire:<20} | {esas:<12} | {karar:<12} | {tarih:<12}")

        return results

    def read_decision(self, decision_id: int, results_cache: list = None):
        """
        Lazy loads the full text.
        1. Find the URL for the given ID (from cache or DB query).
        2. Download the JSON from HF.
        3. Filter the specific record from that JSON.
        """
        target_url = None
        
        # Check cache first
        if results_cache:
            for r in results_cache:
                if str(r['id']) == str(decision_id):
                    target_url = r.get('full_text_url')
                    break
        
        # If not in cache, query DB
        if not target_url:
            res = self.db.search_decisions(id=decision_id)
            if res:
                target_url = res[0].get('full_text_url')
        
        if not target_url:
            print("Decision ID not found or has no URL.")
            return

        print(f"\nFetching full text from storage: {target_url}...")
        batch_data = self.storage.fetch_full_text(target_url)
        
        # Find the specific record in the batch
        record = next((item for item in batch_data if str(item.get('id')) == str(decision_id)), None)
        
        if record:
            print("\n" + "="*50)
            print(f"FULL TEXT FOR DECISION {decision_id}")
            print("="*50)
            print(record.get('icerik_ham', 'No Content'))
            print("="*50)
        else:
            print("Record not found inside the storage file (Data consistency error).")

if __name__ == "__main__":
    reader = Reader()
    
    # Simple CLI
    print("Search Options: keyword (arananKelime), daire, year, date (YYYY-MM-DD)")
    cmd = input("Enter search query (e.g. daire=14. Hukuk, keyword=tazminat): ")
    
    filters = {}
    if cmd:
        parts = cmd.split(',')
        for p in parts:
            if '=' in p:
                k, v = p.split('=')
                k = k.strip()
                v = v.strip()
                
                # Map user friendly keys / search.php keys to our internal DB args
                if k in ['arananKelime', 'keyword', 'q']:
                    filters['keyword'] = v
                elif k in ['birimYrgKurulDaire', 'daire']:
                    filters['daire'] = v
                elif k in ['kararYil', 'year']:
                    filters['year'] = v
                elif k in ['baslangicTarihi', 'start_date']:
                    filters['start_date'] = v
                elif k in ['esas', 'esas_no']:
                    filters['esas_no'] = v
                elif k in ['karar', 'karar_no']:
                    filters['karar_no'] = v
    
    results = reader.search(**filters)
    
    if results:
        sel = input("\nEnter ID to read full text (or press enter to exit): ")
        if sel:
            reader.read_decision(sel, results)
