"""
PDDikti Client — API v4.1.0 (pddikti.rone.dev)
================================================
HTTP client yang menghubungi API PDDikti baru secara langsung.
Strategi:
  - Primary  : https://pddikti.rone.dev/api/
  - Fallback : https://pddikti.fastapicloud.dev/api/
  - Retry    : exponential backoff (1s → 2s → 4s)
  - Rate-limit: hormati retry_after_seconds dari response 503

Semua fungsi bersifat async dan mengembalikan data yang sudah
di-normalisasi ke format yang kompatibel dengan scraper.py.
"""

import asyncio
import json
import logging
import time
import urllib3
from typing import Any, Optional

import requests

# Suppress SSL unverified HTTPS warnings (Windows SSL cert chain issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# ── Konfigurasi Base URL ─────────────────────────────────────────────────────
PRIMARY_BASE    = "https://pddikti.rone.dev/api"
FALLBACK_BASE   = "https://pddikti.fastapicloud.dev/api"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; PDDikti-Scraper/4.0)",
}

TIMEOUT     = 40        # detik per request
MAX_RETRIES = 3         # percobaan per URL
RETRY_DELAY = 1.0       # detik awal (exponential: ×2 tiap percobaan)
REQ_DELAY   = 0.5       # jeda antar request normal (sopan ke server)


# ── Low-level HTTP helper ────────────────────────────────────────────────────

def _get(url: str, retries: int = MAX_RETRIES) -> Optional[Any]:
    """
    Sync HTTP GET dengan retry dan exponential backoff.
    Mengembalikan parsed JSON atau None jika gagal / 503 persisten.
    """
    delay = RETRY_DELAY
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False)

            if resp.status_code == 503:
                # API sedang rate-limited / high traffic
                try:
                    body = resp.json()
                    retry_after = body.get("retry_after_seconds", 0)
                    if retry_after and retry_after > 60:
                        # Primary 503 — jangan retry, langsung return None
                        # agar fallback ke HA endpoint bisa dicoba
                        logger.warning(
                            f"[pddikti_client] 503 dari {url} — "
                            f"retry_after={retry_after}s, coba HA fallback"
                        )
                        return None
                except Exception:
                    pass
                logger.warning(f"[pddikti_client] 503 dari {url}, attempt {attempt+1}/{retries}")
                if attempt < retries - 1:
                    time.sleep(delay)
                    delay *= 2
                continue

            if resp.status_code >= 400:
                logger.debug(f"[pddikti_client] HTTP {resp.status_code} dari {url}")
                return None

            data = resp.json()

            # Tangani format response baru {"success":bool, "data": ...}
            if isinstance(data, dict):
                if data.get("success") is False:
                    logger.debug(f"[pddikti_client] success=false dari {url}")
                    return None
                # Kalau ada key "data", kembalikan isinya
                if "data" in data:
                    return data["data"]

            return data

        except requests.exceptions.ConnectionError as e:
            logger.warning(f"[pddikti_client] ConnectionError {url}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
        except requests.exceptions.Timeout:
            logger.warning(f"[pddikti_client] Timeout {url}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"[pddikti_client] JSON decode error dari {url}")
            return None
        except Exception as e:
            logger.warning(f"[pddikti_client] Error tak terduga {url}: {e}")
            return None

    return None


def _fetch(path: str) -> Optional[Any]:
    """
    Fetch endpoint PDDikti dengan auto-fallback ke HA endpoint.
    path sudah dimulai dengan '/'  contoh: '/search/prodi/zakat'
    """
    primary_url  = f"{PRIMARY_BASE}{path}"
    fallback_url = f"{FALLBACK_BASE}{path}"

    result = _get(primary_url)
    if result is not None:
        return result

    # Coba fallback
    logger.info(f"[pddikti_client] Primary gagal, fallback ke HA: {fallback_url}")
    return _get(fallback_url)


# ── Public async API ─────────────────────────────────────────────────────────

async def check_api_status() -> dict:
    """
    Cek status API PDDikti sebelum mulai scraping.
    Returns dict: {available: bool, state: str, message: str}
    """
    try:
        primary_url = f"{PRIMARY_BASE}/"
        resp = requests.get(primary_url, headers=HEADERS, timeout=10, verify=False)
        body = resp.json()
        availability = body.get("availability", {})
        state = availability.get("state", "unknown")
        msg   = availability.get("message", "")
        if state == "available":
            return {"available": True, "state": state, "message": msg}
        elif state == "limited":
            # Masih bisa berjalan walau lambat — gunakan HA endpoint
            limited_eps = availability.get("limited_endpoints", [])
            return {
                "available": True,
                "state": state,
                "message": msg,
                "limited_endpoints": limited_eps,
                "note": "Gunakan HA endpoint: pddikti.fastapicloud.dev",
            }
        else:
            return {"available": False, "state": state, "message": msg}
    except Exception as e:
        return {"available": True, "state": "unknown", "message": str(e)}


async def search_prodi(keyword: str) -> list:
    """
    Cari program studi berdasarkan keyword.
    Endpoint: GET /search/prodi/{keyword}/
    Returns list of dicts: [{id, nama, pt, pt_singkat, jenjang, ...}]
    """
    path = f"/search/prodi/{requests.utils.quote(keyword, safe='')}/"
    result = await asyncio.to_thread(_fetch, path)
    if not isinstance(result, list):
        return []

    normalized = []
    for item in result:
        if not isinstance(item, dict):
            continue
        pid  = str(item.get("id") or item.get("kode_prodi") or "").strip()
        nama = str(item.get("nama") or item.get("nama_prodi") or "").strip()
        pt   = str(item.get("pt")  or item.get("nama_pt")    or "").strip()
        if not pid or not nama:
            continue
        normalized.append({
            "id":         pid,
            "nama":       nama,
            "jenjang":    str(item.get("jenjang") or "").strip(),
            "pt":         pt,
            "pt_singkat": str(item.get("pt_singkat") or "").strip(),
            "id_sp":      str(item.get("id_sp") or "").strip(),   # PT ID
        })
    return normalized


async def search_dosen(keyword: str) -> list:
    """
    Cari dosen berdasarkan keyword (nama / NIDN).
    Endpoint: GET /search/dosen/{keyword}/
    Returns list of dicts: [{id, nidn, nama, nama_pt, ...}]
    """
    path = f"/search/dosen/{requests.utils.quote(keyword, safe='')}/"
    result = await asyncio.to_thread(_fetch, path)
    if not isinstance(result, list):
        return []
    return result


async def get_dosen_homebase(prodi_id: str, semester: str) -> list:
    """
    Ambil dosen homebase suatu prodi pada semester tertentu.
    Endpoint: GET /prodi/dosen-homebase/{id_prodi}/{id_thsmt}/
    Returns list dosen dicts.
    """
    path = f"/prodi/dosen-homebase/{prodi_id}/{semester}/"
    result = await asyncio.to_thread(_fetch, path)
    if not isinstance(result, list):
        return []
    return result


async def get_dosen_penghitung_ratio(prodi_id: str, semester: str) -> list:
    """
    Ambil dosen yang dipakai untuk perhitungan rasio (termasuk dosen penugasan).
    Endpoint: GET /prodi/dosen-penghitung-ratio/{id_prodi}/{id_thsmt}/
    Returns list dosen dicts — fallback/pelengkap dari homebase.
    """
    path = f"/prodi/dosen-penghitung-ratio/{prodi_id}/{semester}/"
    result = await asyncio.to_thread(_fetch, path)
    if not isinstance(result, list):
        return []
    return result


async def get_dosen_profile(dosen_id: str) -> Optional[dict]:
    """
    Ambil profil lengkap dosen berdasarkan pddikti_id.
    Endpoint: GET /dosen/profile/{id_dosen}/
    Returns dict profil atau None.
    """
    path = f"/dosen/profile/{dosen_id}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, dict):
        return result
    return None


async def get_prodi_detail(prodi_id: str) -> Optional[dict]:
    """
    Ambil detail program studi (akreditasi, status, kode, id_sp/PT ID).
    Endpoint: GET /prodi/detail/{id_prodi}/
    Returns dict atau None.
    """
    path = f"/prodi/detail/{prodi_id}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, dict):
        return result
    # API baru kadang mengembalikan list dengan 1 elemen
    if isinstance(result, list) and result:
        return result[0] if isinstance(result[0], dict) else None
    return None


async def get_pt_detail(pt_id: str) -> Optional[dict]:
    """
    Ambil detail perguruan tinggi (status, provinsi, kelompok, pembina).
    Endpoint: GET /pt/detail/{id_pt}/
    Returns dict atau None.
    """
    path = f"/pt/detail/{pt_id}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, dict):
        return result
    return None


async def get_pt_riwayat(pt_id: str) -> list:
    """
    Ambil riwayat perubahan nama perguruan tinggi (STAIN→IAIN→UIN).
    Endpoint: GET /pt/riwayat/{id_pt}/
    Returns list riwayat atau [].
    """
    path = f"/pt/riwayat/{pt_id}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, list):
        return result
    return []


async def get_prodi_riwayat(prodi_id: str) -> list:
    """
    Ambil riwayat perubahan nama program studi.
    Endpoint: GET /prodi/riwayat/{id_prodi}/
    Returns list riwayat atau [].
    """
    path = f"/prodi/riwayat/{prodi_id}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, list):
        return result
    return []


async def get_prodi_by_bidang_ilmu(bidang: str) -> list:
    """
    Dapatkan semua prodi dalam satu bidang ilmu.
    Endpoint: GET /prodi-bidang-ilmu/{bidang}/
    bidang: agama | ekonomi | humaniora | kesehatan | mipa |
            pendidikan | pertanian | seni | sosial | teknik
    Returns list prodi dicts.
    """
    path = f"/prodi-bidang-ilmu/{bidang}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, list):
        return result
    return []


async def get_prodi_num_students_lecturers(prodi_id: str) -> Optional[dict]:
    """
    Dapatkan jumlah mahasiswa & dosen di prodi.
    Endpoint: GET /prodi/num-students-lecturers/{id_prodi}/
    Returns dict atau None.
    """
    path = f"/prodi/num-students-lecturers/{prodi_id}/"
    result = await asyncio.to_thread(_fetch, path)
    if isinstance(result, dict):
        return result
    return None


# ── Normalisasi data respons API baru ────────────────────────────────────────

def normalize_dosen_from_list(raw: dict) -> dict:
    """
    Normalisasi satu record dosen dari response dosen-homebase / penghitung-ratio
    ke format yang kompatibel dengan scraper.py.

    Format API baru (contoh):
    {
      "id": "abc-123-uuid",
      "nidn": "1234567890",
      "nuptk": "",
      "nama": "Dr. Ahmad Fauzi",  atau "nama_dosen"
      "jenis_kelamin": "L",
      "pendidikan_tertinggi": "S3",
      "jabatan_akademik": "Lektor Kepala",
      "status_aktivitas": "Aktif",
      "status_ikatan_kerja": "Dosen Tetap",
      "nama_pt": "UIN Sunan Kalijaga",
    }
    """
    return {
        "id":                  str(raw.get("id") or "").strip(),       # pddikti_id
        "nidn":                str(raw.get("nidn") or "").strip(),
        "nuptk":               str(raw.get("nuptk") or "").strip(),
        "nama_dosen":          str(raw.get("nama_dosen") or raw.get("nama") or "").strip(),
        "jenis_kelamin":       str(raw.get("jenis_kelamin") or "").strip(),
        "pendidikan":          str(raw.get("pendidikan_tertinggi") or raw.get("pendidikan") or "").strip(),
        "jabatan_fungsional":  str(raw.get("jabatan_akademik") or raw.get("jabatan_fungsional") or "").strip(),
        "status_aktif":        str(raw.get("status_aktivitas") or raw.get("status_aktif") or "").strip(),
        "status_pegawai":      str(raw.get("status_kepegawaian") or raw.get("status_pegawai") or "").strip(),
        "ikatan_kerja":        str(raw.get("status_ikatan_kerja") or raw.get("ikatan_kerja") or "").strip(),
        "nama_pt":             str(raw.get("nama_pt") or raw.get("pt") or "").strip(),
    }


def normalize_dosen_profile(raw: dict) -> dict:
    """
    Normalisasi response profil dosen dari /api/dosen/profile/{id}/.
    """
    return {
        "id":                    str(raw.get("id") or "").strip(),
        "nidn":                  str(raw.get("nidn") or "").strip(),
        "nuptk":                 str(raw.get("nuptk") or "").strip(),
        "nama_dosen":            str(raw.get("nama_dosen") or raw.get("nama") or "").strip(),
        "jenis_kelamin":         str(raw.get("jenis_kelamin") or "").strip(),
        "pendidikan_tertinggi":  str(raw.get("pendidikan_tertinggi") or "").strip(),
        "jabatan_akademik":      str(raw.get("jabatan_akademik") or raw.get("jabatan_fungsional") or "").strip(),
        "status_aktivitas":      str(raw.get("status_aktivitas") or "").strip(),
        "status_ikatan_kerja":   str(raw.get("status_ikatan_kerja") or "").strip(),
        "nama_pt":               str(raw.get("nama_pt") or "").strip(),
        "nama_prodi":            str(raw.get("nama_prodi") or "").strip(),
    }


def normalize_prodi_detail(raw: dict) -> dict:
    """
    Normalisasi response detail prodi dari /api/prodi/detail/{id}/.
    """
    return {
        "kode_prodi":        str(raw.get("kode_prodi") or "").strip(),
        "status":            str(raw.get("status") or "Aktif").strip(),
        "akreditasi":        str(raw.get("akreditasi") or "").strip(),
        "status_akreditasi": str(raw.get("status_akreditasi") or "").strip(),
        "provinsi":          str(raw.get("provinsi") or "").strip(),
        "id_sp":             str(raw.get("id_sp") or raw.get("pt_id") or "").strip(),
        "nama_prodi":        str(raw.get("nama") or raw.get("nama_prodi") or "").strip(),
        "jenjang":           str(raw.get("jenjang") or "").strip(),
    }


def normalize_pt_detail(raw: dict) -> dict:
    """
    Normalisasi response detail PT dari /api/pt/detail/{id}/.
    """
    return {
        "status_pt":    str(raw.get("status_pt") or raw.get("bentuk_pt") or "").strip(),
        "kelompok":     str(raw.get("kelompok") or "").strip(),
        "pembina":      str(raw.get("pembina") or "").strip(),
        "provinsi":     str(raw.get("provinsi_pt") or raw.get("provinsi") or "").strip(),
        "akreditasi":   str(raw.get("akreditasi_pt") or raw.get("akreditasi") or "").strip(),
        "nama":         str(raw.get("nama_pt") or raw.get("nama") or "").strip(),
    }
