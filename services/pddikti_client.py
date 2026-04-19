"""
PDDikti Client Wrapper — menggunakan library pddiktipy sebagai tambahan
dari scraper HTTP langsung yang sudah ada.

Library pddiktipy menyediakan ~63 method untuk mengakses data PDDikti.
Client ini menjadi fasad yang:
  1. Mencoba pddiktipy terlebih dahulu
  2. Fallback ke HTTP langsung jika pddiktipy gagal/tidak tersedia
"""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Cek ketersediaan pddiktipy ──────────────────────────────────────────────
try:
    from pddiktipy import api as PddiktiApi
    PDDIKTIPY_AVAILABLE = True
    logger.info("[pddikti_client] pddiktipy library tersedia ✅")
except ImportError:
    PDDIKTIPY_AVAILABLE = False
    logger.warning("[pddikti_client] pddiktipy tidak tersedia, menggunakan HTTP fallback ⚠️")


# ── Public API ───────────────────────────────────────────────────────────────

async def search_prodi(keyword: str) -> list:
    """
    Cari program studi berdasarkan keyword.
    Menggunakan pddiktipy jika tersedia, fallback ke HTTP langsung.
    Returns list of prodi dicts: [{id, nama, jenjang, pt, pt_singkat, ...}]
    """
    if PDDIKTIPY_AVAILABLE:
        try:
            result = await asyncio.to_thread(_search_prodi_pddiktipy, keyword)
            if result:
                return result
        except Exception as e:
            logger.warning(f"[pddikti_client] pddiktipy search_prodi error: {e}, fallback ke HTTP")

    return _search_prodi_http(keyword)


async def get_dosen_prodi(prodi_id: str, endpoint: str = "homebase", semester: Optional[str] = None) -> list:
    """
    Dapatkan dosen dari satu prodi via homebase atau penugasan.
    endpoint: "homebase" | "penugasan"
    Menggunakan pddiktipy jika tersedia, fallback ke HTTP langsung.
    Returns list of dosen dicts.
    """
    if PDDIKTIPY_AVAILABLE:
        try:
            result = await asyncio.to_thread(_get_dosen_pddiktipy, prodi_id, endpoint, semester)
            if result is not None:
                return result
        except Exception as e:
            logger.warning(f"[pddikti_client] pddiktipy get_dosen error: {e}, fallback ke HTTP")

    return _get_dosen_http(prodi_id, endpoint, semester)


async def get_prodi_detail(prodi_id: str) -> Optional[dict]:
    """Dapatkan detail program studi (kode, akreditasi, status, provinsi, dll)."""
    if PDDIKTIPY_AVAILABLE:
        try:
            result = await asyncio.to_thread(_get_prodi_detail_pddiktipy, prodi_id)
            if result:
                return result
        except Exception as e:
            logger.warning(f"[pddikti_client] pddiktipy get_prodi_detail error: {e}, fallback ke HTTP")

    return _get_prodi_detail_http(prodi_id)


async def get_pt_detail(pt_id: str) -> Optional[dict]:
    """Dapatkan detail perguruan tinggi (status, kelompok, pembina, provinsi)."""
    if PDDIKTIPY_AVAILABLE:
        try:
            result = await asyncio.to_thread(_get_pt_detail_pddiktipy, pt_id)
            if result:
                return result
        except Exception as e:
            logger.warning(f"[pddikti_client] pddiktipy get_pt_detail error: {e}, fallback ke HTTP")

    return _get_pt_detail_http(pt_id)


async def get_dosen_profile(nidn: str) -> Optional[dict]:
    """
    Dapatkan profil lengkap dosen berdasarkan NIDN menggunakan pddiktipy.
    Method ini HANYA tersedia via pddiktipy (tidak ada di scraper HTTP lama).
    Returns dict profil dosen atau None.
    """
    if not PDDIKTIPY_AVAILABLE:
        return None
    try:
        result = await asyncio.to_thread(_get_dosen_profile_pddiktipy, nidn)
        return result
    except Exception as e:
        logger.warning(f"[pddikti_client] get_dosen_profile error untuk NIDN {nidn}: {e}")
        return None


def pddiktipy_available() -> bool:
    """Cek apakah pddiktipy tersedia di lingkungan ini."""
    return PDDIKTIPY_AVAILABLE


# ── pddiktipy implementations ────────────────────────────────────────────────

def _search_prodi_pddiktipy(keyword: str) -> list:
    """Implementasi search prodi via pddiktipy (sync, dijalankan di thread)."""
    with PddiktiApi() as client:
        # pddiktipy: search_prodi atau search_all tergantung versi
        raw = None
        if hasattr(client, "search_prodi"):
            raw = client.search_prodi(keyword)
        elif hasattr(client, "search_all"):
            data = client.search_all(keyword)
            # Ambil bagian prodi jika ada
            if isinstance(data, dict):
                raw = data.get("prodi") or data.get("program_studi") or []
            elif isinstance(data, list):
                raw = data

        if not raw or not isinstance(raw, list):
            return []

        # Normalisasi ke format standar scraper
        normalized = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            normalized.append({
                "id": str(item.get("id") or item.get("kode_prodi") or "").strip(),
                "nama": str(item.get("nama") or item.get("nama_prodi") or "").strip(),
                "jenjang": str(item.get("jenjang") or "").strip(),
                "pt": str(item.get("pt") or item.get("nama_pt") or "").strip(),
                "pt_singkat": str(item.get("pt_singkat") or "").strip(),
            })
        return [n for n in normalized if n["id"] and n["nama"]]


def _get_dosen_pddiktipy(prodi_id: str, endpoint: str, semester: Optional[str]) -> list:
    """Implementasi get dosen via pddiktipy (sync, dijalankan di thread)."""
    with PddiktiApi() as client:
        raw = None
        if endpoint == "homebase":
            method = getattr(client, "get_dosen_homebase", None) or getattr(client, "dosen_homebase", None)
        else:
            method = getattr(client, "get_dosen_penugasan", None) or getattr(client, "dosen_penugasan", None)

        if method is None:
            return []

        if semester:
            try:
                raw = method(prodi_id, semester=semester)
            except TypeError:
                raw = method(prodi_id)
        else:
            raw = method(prodi_id)

        if not raw or not isinstance(raw, list):
            return []

        # Normalisasi ke format standar scraper
        normalized = []
        for d in raw:
            if not isinstance(d, dict):
                continue
            normalized.append({
                "nidn": str(d.get("nidn") or "").strip(),
                "nama_dosen": str(d.get("nama_dosen") or d.get("nama") or "").strip(),
                "nuptk": str(d.get("nuptk") or "").strip(),
                "pendidikan": str(d.get("pendidikan") or d.get("pendidikan_terakhir") or "").strip(),
                "jabatan_fungsional": str(d.get("jabatan_fungsional") or "").strip(),
                "status_aktif": str(d.get("status_aktif") or d.get("status_aktivitas") or "").strip(),
                "status_pegawai": str(d.get("status_pegawai") or d.get("status_kepegawaian") or "").strip(),
                "ikatan_kerja": str(d.get("ikatan_kerja") or d.get("status_ikatan_kerja") or "").strip(),
                "jenis_kelamin": str(d.get("jenis_kelamin") or "").strip(),
                "nama_pt": str(d.get("nama_pt") or d.get("pt") or "").strip(),
            })
        return [n for n in normalized if n["nama_dosen"]]


def _get_prodi_detail_pddiktipy(prodi_id: str) -> Optional[dict]:
    """Implementasi get prodi detail via pddiktipy (sync)."""
    with PddiktiApi() as client:
        method = getattr(client, "get_prodi_detail", None) or getattr(client, "prodi_detail", None)
        if method is None:
            return None
        raw = method(prodi_id)
        if not raw or not isinstance(raw, dict):
            return None
        return {
            "kode_prodi": str(raw.get("kode_prodi") or "").strip(),
            "status": str(raw.get("status") or "Aktif").strip(),
            "akreditasi": str(raw.get("akreditasi") or "").strip(),
            "status_akreditasi": str(raw.get("status_akreditasi") or "").strip(),
            "provinsi": str(raw.get("provinsi") or "").strip(),
            "id_sp": str(raw.get("id_sp") or raw.get("pt_id") or "").strip(),
        }


def _get_pt_detail_pddiktipy(pt_id: str) -> Optional[dict]:
    """Implementasi get PT detail via pddiktipy (sync)."""
    with PddiktiApi() as client:
        method = getattr(client, "get_pt_detail", None) or getattr(client, "pt_detail", None)
        if method is None:
            return None
        raw = method(pt_id)
        if not raw or not isinstance(raw, dict):
            return None
        return {
            "status_pt": str(raw.get("status_pt") or "").strip(),
            "kelompok": str(raw.get("kelompok") or "").strip(),
            "pembina": str(raw.get("pembina") or "").strip(),
            "provinsi": str(raw.get("provinsi_pt") or raw.get("provinsi") or "").strip(),
            "akreditasi_pt": str(raw.get("akreditasi_pt") or "").strip(),
        }


def _get_dosen_profile_pddiktipy(nidn: str) -> Optional[dict]:
    """Dapatkan profil lengkap dosen via pddiktipy (sync)."""
    with PddiktiApi() as client:
        method = getattr(client, "get_dosen", None) or getattr(client, "search_dosen", None)
        if method is None:
            return None
        raw = method(nidn)
        if not raw:
            return None
        if isinstance(raw, list) and len(raw) > 0:
            raw = raw[0]
        if not isinstance(raw, dict):
            return None
        return raw


# ── HTTP fallback implementations ─────────────────────────────────────────────
# Menggunakan logika yang sama dengan scraper.py eksisting

import requests
import json
import time

_BASE_URL = "https://api-pddikti.kemdiktisaintek.go.id"
_HEADERS = {
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
    "Referer": "https://pddikti.kemdiktisaintek.go.id/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}
_TIMEOUT = 40
_MAX_RETRIES = 3


def _fetch(endpoint: str):
    """Fetch dari PDDikti API dengan retry."""
    url = f"{_BASE_URL}/{endpoint}"
    for attempt in range(_MAX_RETRIES):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            if r.status_code >= 400:
                return None
            data = r.json()
            if isinstance(data, dict) and data.get("message") == "Not Found":
                return None
            return data
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            if attempt < _MAX_RETRIES - 1:
                time.sleep(2 * (attempt + 1))
    return None


def _search_prodi_http(keyword: str) -> list:
    result = _fetch(f"pencarian/prodi/{keyword}")
    if not isinstance(result, list):
        return []
    return result


def _get_dosen_http(prodi_id: str, endpoint: str, semester: Optional[str]) -> list:
    url = f"dosen/{endpoint}/{prodi_id}"
    if semester:
        url += f"?semester={semester}"
    result = _fetch(url)
    if not isinstance(result, list):
        return []
    return result


def _get_prodi_detail_http(prodi_id: str) -> Optional[dict]:
    return _fetch(f"prodi/detail/{prodi_id}")


def _get_pt_detail_http(pt_id: str) -> Optional[dict]:
    return _fetch(f"pt/detail/{pt_id}")
