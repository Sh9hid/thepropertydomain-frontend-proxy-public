import os
import sys
import asyncio
import logging
import json
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add backend to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import core.config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ls_stock_linker")

# Create async engine
engine = create_async_engine(core.config.DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

def normalize_addr(addr: str) -> str:
    if not addr: return ""
    # Remove unit numbers, lowercase, remove punctuation
    addr = addr.lower().split(",")[0]
    addr = "".join(c for c in addr if c.isalnum() or c.isspace())
    # Remove common words
    for word in ["unit", "level", "shop", "suite"]:
        addr = addr.replace(word, "")
    return " ".join(addr.split())

async def link_stock_photos():
    stock_root = Path(core.config.STOCK_ROOT)
    if not stock_root.exists():
        logger.error(f"Stock root {stock_root} not found.")
        return

    async with async_session() as session:
        # Get all leads missing professional photos
        result = await session.execute(
            text("SELECT id, address, suburb FROM leads WHERE main_image IS NULL OR main_image = '' OR main_image LIKE '%static-maps%'")
        )
        leads = result.mappings().all()
        logger.info(f"Checking {len(leads)} leads against L+S Stock...")

        linked_count = 0
        
        # 1. Map out all property folders in stock root
        # Structure: Suburb/Address/Photos...
        # Or Address BH/Photos...
        
        # 1. Map out ALL property folders anywhere in the root, recursively
        logger.info("Deep scanning L+S Stock directory (this may take a minute)...")
        property_folders = []
        for root, dirs, files in os.walk(stock_root):
            for d in dirs:
                norm_d = normalize_addr(d)
                # Does the folder name look like an address? (contains st, rd, etc)
                if any(kw in norm_d for kw in ["st", "rd", "ave", "dr", "pl", "close", "cres", "way", "lane", "ct", "hwy"]):
                    property_folders.append({
                        "path": Path(root) / d,
                        "norm_address": norm_d
                    })

        logger.info(f"Found {len(property_folders)} candidate address folders in Stock.")

        for lead in leads:
            lead_addr_norm = normalize_addr(lead['address'])
            if not lead_addr_norm: continue
            
            best_match = None
            for folder in property_folders:
                # Direct match or partial match
                if lead_addr_norm == folder['norm_address'] or \
                   (len(lead_addr_norm) > 10 and lead_addr_norm in folder['norm_address']) or \
                   (len(folder['norm_address']) > 10 and folder['norm_address'] in lead_addr_norm):
                    best_match = folder
                    break
            
            if best_match:
                # Found a folder! Look for images
                images = list(best_match['path'].rglob("*.jpg")) + \
                         list(best_match['path'].rglob("*.jpeg")) + \
                         list(best_match['path'].rglob("*.png"))
                
                if images:
                    # Pick the first one as main
                    # Get relative path for frontend mount
                    rel_path = "/stock_photos/" + str(images[0].relative_to(stock_root)).replace("\\", "/")
                    all_rel = ["/stock_photos/" + str(img.relative_to(stock_root)).replace("\\", "/") for img in images[:10]]
                    
                    logger.info(f"✅ LINKED STOCK PHOTO: {lead['address']} -> {rel_path}")
                    await session.execute(
                        text("UPDATE leads SET main_image = :main, property_images = :imgs WHERE id = :id"),
                        {"main": rel_path, "imgs": json.dumps(all_rel), "id": lead['id']}
                    )
                    linked_count += 1
                    if linked_count % 10 == 0: await session.commit()

        await session.commit()
        logger.info(f"Finished! Linked {linked_count} leads to official L+S stock photos.")

if __name__ == "__main__":
    asyncio.run(link_stock_photos())
