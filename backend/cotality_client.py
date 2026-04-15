import os
import json
import sqlite3
import httpx
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from core import config

DB_PATH = config.DB_PATH

class CotalityClient:
    def _generate_pseudo_id(self, address: str):
        import hashlib
        return hashlib.md5(address.lower().strip().encode()).hexdigest()

    def _get_cache(self, table: str, pk_col: str, pk_val: str):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(f"SELECT * FROM {table} WHERE {pk_col} = ?", (pk_val,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def _save_cache(self, table: str, data: Dict[str, Any]):
        conn = sqlite3.connect(DB_PATH)
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        upsert_stmt = ", ".join([f"{k}=excluded.{k}" for k in data.keys() if k != 'property_id' and k != 'suburb_id' and k != 'id'])
        
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT DO UPDATE SET {upsert_stmt}"
        conn.execute(sql, list(data.values()))
        conn.commit()
        conn.close()

    async def get_property_intelligence(self, address: str):
        property_id = self._generate_pseudo_id(address) 
        cached = self._get_cache("property_cache", "property_id", property_id)
        if cached and cached['last_updated_attributes']:
            return {
                "property_id": cached['property_id'],
                "address": cached['address'],
                "attributes": json.loads(cached['attributes_json']),
                "sales": json.loads(cached['sales_history_json']),
                "avm": json.loads(cached['avm_json']),
                "is_from_cache": True
            }

        # Simulated response logic (to be linked to real keys)
        intel = {
            "property_id": property_id,
            "address": address,
            "attributes": {"beds": 4, "baths": 2, "cars": 2, "land_sqm": 450, "year_built": 2015},
            "sales": [{"date": "2020-05-10", "price": 850000}],
            "avm": {"estimate": 1250000, "low": 1180000, "high": 1320000, "confidence": "High"}
        }
        
        self._save_cache("property_cache", {
            "property_id": property_id,
            "address": address,
            "attributes_json": json.dumps(intel['attributes']),
            "sales_history_json": json.dumps(intel['sales']),
            "avm_json": json.dumps(intel['avm']),
            "last_updated_attributes": datetime.now().isoformat(),
            "last_updated_avm": datetime.now().isoformat()
        })
        return intel

    async def add_to_portfolio(self, address: str, notes: str = ""):
        property_id = self._generate_pseudo_id(address)
        self._save_cache("portfolios", {
            "property_id": property_id,
            "address": address,
            "tracking_notes": notes
        })
        return {"status": "tracked", "property_id": property_id}

    async def get_portfolio(self):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM portfolios").fetchall()
        conn.close()
        return [dict(r) for r in rows]
