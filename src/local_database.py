import sqlite3
import os
from typing import List, Dict, Any

class LocalDatabaseHandler:
    def __init__(self):
        self.db_path = "local_metadata.db"
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        # Mimic the Supabase schema
        c.execute('''
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY,
                daire TEXT,
                esas_no TEXT,
                karar_no TEXT,
                karar_tarihi TEXT,
                ozet TEXT,
                full_text_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def upsert_metadata(self, metadata_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Batch insert or REPLACE into SQLite.
        """
        if not metadata_list:
            return {"data": [], "count": 0}
            
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        try:
            for item in metadata_list:
                # Use REPLACE INTO to handle upsert logic (simpler for SQLite)
                c.execute('''
                    INSERT OR REPLACE INTO decisions (id, daire, esas_no, karar_no, karar_tarihi, ozet, full_text_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    item['id'],
                    item.get('daire'),
                    item.get('esas_no'),
                    item.get('karar_no'),
                    item.get('karar_tarihi'),
                    item.get('ozet'),
                    item.get('full_text_url')
                ))
            conn.commit()
            return {"data": metadata_list, "count": len(metadata_list)}
        except Exception as e:
            print(f"[LOCAL] Error upserting metadata: {e}")
            raise e
        finally:
            conn.close()

    def search_decisions(self, **kwargs) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row # To access columns by name
        c = conn.cursor()
        
        query = "SELECT * FROM decisions WHERE 1=1"
        params = []
        
        if kwargs.get('id'):
            query += " AND id = ?"
            params.append(kwargs['id'])
            
        if kwargs.get('daire'):
            query += " AND daire LIKE ?"
            params.append(f"%{kwargs['daire']}%")
            
        if kwargs.get('esas_no'):
            query += " AND esas_no = ?"
            params.append(kwargs['esas_no'])
            
        if kwargs.get('karar_no'):
            query += " AND karar_no = ?"
            params.append(kwargs['karar_no'])
            
        if kwargs.get('keyword'):
            # Mimic 'arananKelime' by searching in summary or daire
            # Note: Full text search not available in SQLite/Supabase Metadata-only setup easily
            # We search in 'ozet' and 'daire' as a proxy.
            query += " AND (ozet LIKE ? OR daire LIKE ?)"
            term = f"%{kwargs['keyword']}%"
            params.append(term)
            params.append(term)

        if kwargs.get('year'):
            # Filter by YYYY in karar_tarihi
            query += " AND strftime('%Y', karar_tarihi) = ?"
            params.append(str(kwargs['year']))
            
        if kwargs.get('start_date'):
            query += " AND karar_tarihi >= ?"
            params.append(kwargs['start_date'])

        query += " LIMIT 20"
        
        c.execute(query, params)
        rows = c.fetchall()
        
        # Convert to dict
        results = [dict(row) for row in rows]
        conn.close()
        return results
