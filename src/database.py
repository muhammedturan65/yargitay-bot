import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from typing import List, Dict, Any
from .config import Config

class DatabaseHandler:
    def __init__(self):
        Config.validate()
        self.conn_str = Config.DATABASE_URL
        self.table_name = "decisions"
        self.init_db()

    def get_connection(self):
        return psycopg2.connect(self.conn_str)

    def init_db(self):
        """
        Creates the 'decisions' table if it doesn't exist.
        """
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id BIGINT PRIMARY KEY,
            daire TEXT,
            esas_no TEXT,
            karar_no TEXT,
            karar_tarihi DATE,
            ozet TEXT,
            full_text_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        -- Create indexes for faster search
        CREATE INDEX IF NOT EXISTS idx_daire ON {self.table_name}(daire);
        CREATE INDEX IF NOT EXISTS idx_esas_no ON {self.table_name}(esas_no);
        CREATE INDEX IF NOT EXISTS idx_karar_no ON {self.table_name}(karar_no);
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_query)
                conn.commit()
            print("Database initialized (tables verified).")
        except Exception as e:
            print(f"Error initializing database: {e}")
            raise e

    def upsert_metadata(self, metadata_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Batch insert or update metadata in Supabase (PostgreSQL).
        """
        if not metadata_list:
            return {"count": 0}

        query = f"""
        INSERT INTO {self.table_name} (id, daire, esas_no, karar_no, karar_tarihi, ozet, full_text_url)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            daire = EXCLUDED.daire,
            esas_no = EXCLUDED.esas_no,
            karar_no = EXCLUDED.karar_no,
            karar_tarihi = EXCLUDED.karar_tarihi,
            ozet = EXCLUDED.ozet,
            full_text_url = EXCLUDED.full_text_url;
        """
        
        # Prepare data tuple list for execute_values
        values = [
            (
                m.get('id'),
                m.get('daire'),
                m.get('esas_no'),
                m.get('karar_no'),
                m.get('karar_tarihi'),
                m.get('ozet'),
                m.get('full_text_url')
            )
            for m in metadata_list
        ]

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, query, values)
                conn.commit()
            return {"count": len(metadata_list)}
        except Exception as e:
            print(f"Error upserting metadata: {e}")
            raise e

    def search_decisions(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Search for decisions based on provided filters.
        """
        query_parts = [f"SELECT * FROM {self.table_name} WHERE 1=1"]
        params = []

        if kwargs.get('id'):
            query_parts.append("AND id = %s")
            params.append(kwargs['id'])

        if kwargs.get('daire'):
            query_parts.append("AND daire ILIKE %s")
            params.append(f"%{kwargs['daire']}%")

        if kwargs.get('esas_no'):
            query_parts.append("AND esas_no = %s")
            params.append(kwargs['esas_no'])
            
        if kwargs.get('karar_no'):
            query_parts.append("AND karar_no = %s")
            params.append(kwargs['karar_no'])

        if kwargs.get('keyword'):
            query_parts.append("AND ozet ILIKE %s")
            params.append(f"%{kwargs['keyword']}%")
            
        if kwargs.get('year'):
            query_parts.append("AND karar_tarihi >= %s AND karar_tarihi <= %s")
            params.append(f"{kwargs['year']}-01-01")
            params.append(f"{kwargs['year']}-12-31")

        if kwargs.get('start_date'):
            query_parts.append("AND karar_tarihi >= %s")
            params.append(kwargs['start_date'])

        if kwargs.get('end_date'):
            query_parts.append("AND karar_tarihi <= %s")
            params.append(kwargs['end_date'])

        query_parts.append("LIMIT 20")
        
        final_query = " ".join(query_parts)

        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(final_query, params)
                    results = cur.fetchall()
            return results
        except Exception as e:
            print(f"Error searching decisions: {e}")
            return []
