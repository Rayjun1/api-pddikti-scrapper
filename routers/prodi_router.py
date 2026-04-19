"""
PDDikti Dosen Explorer — Prodi Router
Serve prodi target list for the scraping UI.
"""

import json
import os
from typing import Optional

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/prodi", tags=["prodi"])

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data")
PRODI_LIST_FILE = os.path.join(DATA_DIR, "prodi_target_list.json")


@router.get("/rumpun")
async def get_rumpun_options():
    """Get the 12 official rumpun categories for the search/filter UI."""
    from services.scraper import RUMPUN_PRODI_RESMI
    return {"rumpun": RUMPUN_PRODI_RESMI}


@router.get("/targets")
async def get_prodi_targets(q: Optional[str] = Query(None)):
    """
    Get prodi target list (optionally filtered by search query).
    This is public so the scraping UI can show available prodi.
    """
    if not os.path.exists(PRODI_LIST_FILE):
        return {"targets": [], "total": 0}

    with open(PRODI_LIST_FILE, "r") as f:
        all_targets = json.load(f)

    if q:
        q_upper = q.strip().upper()
        filtered = [
            t for t in all_targets
            if q_upper in t["prodi"].upper() or q_upper in t.get("pt", "").upper()
        ]
    else:
        filtered = all_targets

    # Group by rumpun for summary
    from services.scraper import normalize_prodi_name
    rumpun_counts = {}
    for t in all_targets:
        r = normalize_prodi_name(t["prodi"])
        if r:
            rumpun_counts[r] = rumpun_counts.get(r, 0) + 1

    return {
        "targets": filtered[:200],  # Limit to 200 for perf
        "total": len(filtered),
        "total_all": len(all_targets),
        "rumpun_counts": rumpun_counts,
    }
