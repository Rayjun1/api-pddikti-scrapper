"""
PDDikti Dosen Explorer — Scraper Service (API v4.1.0)
=====================================================
Menggunakan API baru pddikti.rone.dev sebagai sumber data.

Alur scraping:
  STEP 1 — Discovery: cari prodi ID per rumpun via search
  STEP 2 — Prodi Detail: ambil info prodi + PT, simpan ke DB
  STEP 3 — Collect Dosen: scan dosen per prodi × per semester
            (homebase + penghitung-ratio sebagai fallback)
  STEP 4 — Profil & Simpan: fetch profil tiap dosen unik, simpan ke DB
"""

import asyncio
import re
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import (
    Dosen, PerguruanTinggi, ProdiDetail, ProgramStudi, ScrapeJob, ScrapeLog
)
import services.pddikti_client as client
from services.pddikti_client import (
    normalize_dosen_from_list,
    normalize_dosen_profile,
    normalize_prodi_detail,
    normalize_pt_detail,
    REQ_DELAY,
)

# ── Konfigurasi Scraper ──────────────────────────────────────────────────────

DEFAULT_SEMESTERS = [
    "20252", "20251", "20242", "20241", "20232", "20231",
    "20222", "20221", "20212", "20211", "20202", "20201",
    "20192", "20191", "20182", "20181", "20172", "20171",
    "20162", "20161", "20152", "20151", "20142", "20141",
    "20132", "20131", "20122", "20121", "20112", "20111",
    "20102", "20101",
]

# Endpoint dosen yang digunakan (primary + fallback)
DOSEN_FETCH_MODES = ["homebase", "penghitung_ratio"]

# ── 22 Rumpun Prodi Resmi Kawasan Ekosyariah ────────────────────────────────
RUMPUN_PRODI_RESMI = [
    "AKUNTANSI SYARIAH",
    "ASURANSI SYARIAH",
    "BISNIS ISLAM",
    "EKONOMI DAN BISNIS ISLAM",
    "EKONOMI ISLAM",
    "EKONOMI SYARIAH",
    "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARIAH (MANAJEMEN SYARIAH)",
    "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "ILMU EKONOMI DAN KEUANGAN ISLAM",
    "ILMU EKONOMI ISLAM",
    "ILMU EKONOMI SYARIAH",
    "KEUANGAN ISLAM TERAPAN",
    "KEUANGAN SYARIAH",
    "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN HAJI DAN UMROH",
    "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH",
    "MANAJEMEN ZAKAT DAN WAKAF",
    "PARIWISATA SYARIAH",
    "PERBANKAN SYARIAH",
    "SAINS EKONOMI ISLAM",
    "ZAKAT DAN WAKAF",
]

# Normalisasi nama prodi dari API → rumpun resmi
PRODI_NORMALIZATION = {
    # AKUNTANSI SYARIAH
    "AKUNTANSI SYARIAH": "AKUNTANSI SYARIAH",
    "AKUNTANSI SYARI'AH": "AKUNTANSI SYARIAH",
    "AKUNTANSI SYARI`AH": "AKUNTANSI SYARIAH",
    "AKUNTANSI SYARI AH": "AKUNTANSI SYARIAH",
    # ASURANSI SYARIAH
    "ASURANSI SYARIAH": "ASURANSI SYARIAH",
    "ASURANSI SYARI'AH": "ASURANSI SYARIAH",
    "ASURANSI SYARI`AH": "ASURANSI SYARIAH",
    # BISNIS ISLAM
    "BISNIS ISLAM": "BISNIS ISLAM",
    # EKONOMI DAN BISNIS ISLAM
    "EKONOMI DAN BISNIS ISLAM": "EKONOMI DAN BISNIS ISLAM",
    # EKONOMI SYARIAH (EKONOMI ISLAM)
    "EKONOMI SYARIAH (EKONOMI ISLAM)": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARIAH / EKONOMI ISLAM": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARI'AH / EKONOMI ISLAM": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARI`AH / EKONOMI ISLAM": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    # EKONOMI SYARIAH (MANAJEMEN SYARIAH)
    "EKONOMI SYARIAH (MANAJEMEN SYARIAH)": "EKONOMI SYARIAH (MANAJEMEN SYARIAH)",
    "EKONOMI SYARI'AH (MANAJEMEN SYARI'AH)": "EKONOMI SYARIAH (MANAJEMEN SYARIAH)",
    # HUKUM EKONOMI SYARIAH (MUAMALAH)
    "HUKUM EKONOMI SYARIAH (MUAMALAH)": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "HUKUM EKONOMI SYARIAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "HUKUM EKONOMI SYARI'AH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "HUKUM EKONOMI SYARI`AH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUAMALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUA'MALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MU'AMALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MU`AMALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUAMALAT": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    # ILMU EKONOMI DAN KEUANGAN ISLAM
    "ILMU EKONOMI DAN KEUANGAN ISLAM": "ILMU EKONOMI DAN KEUANGAN ISLAM",
    "EKONOMI DAN KEUANGAN ISLAM": "ILMU EKONOMI DAN KEUANGAN ISLAM",
    # ILMU EKONOMI ISLAM
    "ILMU EKONOMI ISLAM": "ILMU EKONOMI ISLAM",
    # ILMU EKONOMI SYARIAH
    "ILMU EKONOMI SYARIAH": "ILMU EKONOMI SYARIAH",
    "ILMU EKONOMI SYARI'AH": "ILMU EKONOMI SYARIAH",
    # KEUANGAN ISLAM TERAPAN
    "KEUANGAN ISLAM TERAPAN": "KEUANGAN ISLAM TERAPAN",
    # KEUANGAN SYARIAH
    "KEUANGAN SYARIAH": "KEUANGAN SYARIAH",
    "KEUANGAN SYARI'AH": "KEUANGAN SYARIAH",
    # MANAJEMEN BISNIS SYARIAH
    "MANAJEMEN BISNIS SYARIAH": "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN BISNIS SYARI'AH": "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN DAN BISNIS SYARIAH": "MANAJEMEN BISNIS SYARIAH",
    "BISNIS DAN MANAJEMEN SYARIAH": "MANAJEMEN BISNIS SYARIAH",
    # MANAJEMEN HAJI DAN UMROH
    "MANAJEMEN HAJI DAN UMROH": "MANAJEMEN HAJI DAN UMROH",
    "MANAJEMEN HAJI DAN UMRAH": "MANAJEMEN HAJI DAN UMROH",
    # MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH
    "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH": "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH",
    "MANAJEMEN KEUANGAN DAN PERBANKAN SYARI'AH": "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH",
    # MANAJEMEN ZAKAT DAN WAKAF
    "MANAJEMEN ZAKAT DAN WAKAF": "MANAJEMEN ZAKAT DAN WAKAF",
    # PARIWISATA SYARIAH
    "PARIWISATA SYARIAH": "PARIWISATA SYARIAH",
    "PARIWISATA SYARI'AH": "PARIWISATA SYARIAH",
    # PERBANKAN SYARIAH
    "PERBANKAN SYARIAH": "PERBANKAN SYARIAH",
    "PERBANKAN SYARI'AH": "PERBANKAN SYARIAH",
    "PERBANKAN SYARI`AH": "PERBANKAN SYARIAH",
    # SAINS EKONOMI ISLAM
    "SAINS EKONOMI ISLAM": "SAINS EKONOMI ISLAM",
    # ZAKAT DAN WAKAF
    "ZAKAT DAN WAKAF": "ZAKAT DAN WAKAF",
    "ZAKAT WAKAF": "ZAKAT DAN WAKAF",
    "ZAKAT & WAKAF": "ZAKAT DAN WAKAF",
    # EKONOMI ISLAM (standalone)
    "EKONOMI ISLAM": "EKONOMI ISLAM",
    # EKONOMI SYARIAH (standalone)
    "EKONOMI SYARIAH": "EKONOMI SYARIAH",
    "EKONOMI SYARI'AH": "EKONOMI SYARIAH",
    "EKONOMI SYARI`AH": "EKONOMI SYARIAH",
}

PRODI_SEARCH_VARIANTS = {
    "EKONOMI SYARIAH (EKONOMI ISLAM)": ["EKONOMI SYARIAH", "EKONOMI ISLAM"],
    "EKONOMI SYARIAH (MANAJEMEN SYARIAH)": ["EKONOMI SYARIAH", "MANAJEMEN SYARIAH"],
    "HUKUM EKONOMI SYARIAH (MUAMALAH)": ["HUKUM EKONOMI SYARIAH", "MUAMALAH"],
}

# Normalisasi nama PT — kampus yang berganti nama (STAIN→IAIN→UIN)
PT_NAME_ALIASES: dict[str, str] = {
    # Tulungagung
    "STAIN TULUNGAGUNG": "UNIVERSITAS ISLAM NEGERI SAYYID ALI RAHMATULLAH TULUNGAGUNG",
    "IAIN TULUNGAGUNG": "UNIVERSITAS ISLAM NEGERI SAYYID ALI RAHMATULLAH TULUNGAGUNG",
    "UIN SAYYID ALI RAHMATULLAH TULUNGAGUNG": "UNIVERSITAS ISLAM NEGERI SAYYID ALI RAHMATULLAH TULUNGAGUNG",
    # Palangka Raya
    "IAIN PALANGKARAYA": "UNIVERSITAS ISLAM NEGERI PALANGKA RAYA",
    "IAIN PALANGKA RAYA": "UNIVERSITAS ISLAM NEGERI PALANGKA RAYA",
    "STAIN PALANGKARAYA": "UNIVERSITAS ISLAM NEGERI PALANGKA RAYA",
    # Purwokerto
    "IAIN PURWOKERTO": "UNIVERSITAS ISLAM NEGERI PROFESOR KIAI HAJI SAIFUDDIN ZUHRI PURWOKERTO",
    "STAIN PURWOKERTO": "UNIVERSITAS ISLAM NEGERI PROFESOR KIAI HAJI SAIFUDDIN ZUHRI PURWOKERTO",
    # Jember
    "STAIN JEMBER": "UNIVERSITAS ISLAM NEGERI KIAI HAJI ACHMAD SIDDIQ JEMBER",
    "IAIN JEMBER": "UNIVERSITAS ISLAM NEGERI KIAI HAJI ACHMAD SIDDIQ JEMBER",
    # Ponorogo
    "STAIN PONOROGO": "UNIVERSITAS ISLAM NEGERI KIAI AGENG MUHAMMAD BESARI PONOROGO",
    "IAIN PONOROGO": "UNIVERSITAS ISLAM NEGERI KIAI AGENG MUHAMMAD BESARI PONOROGO",
    # Kudus
    "STAIN KUDUS": "UNIVERSITAS ISLAM NEGERI SUNAN KUDUS",
    "IAIN KUDUS": "UNIVERSITAS ISLAM NEGERI SUNAN KUDUS",
    # Surakarta / Solo
    "IAIN SURAKARTA": "UNIVERSITAS ISLAM NEGERI RADEN MAS SAID SURAKARTA",
    # Padangsidempuan
    "STAIN PADANGSIDEMPUAN": "INSTITUT AGAMA ISLAM NEGERI PADANGSIDIMPUAN",
    # Manado
    "STAIN MANADO": "IAIN MANADO",
    # Parepare
    "STAIN PAREPARE": "INSTITUT AGAMA ISLAM NEGERI PAREPARE",
    # Bengkulu
    "IAIN BENGKULU": "UNIVERSITAS ISLAM NEGERI FATMAWATI SUKARNO BENGKULU",
    # Palembang
    "IAIN RADEN FATAH PALEMBANG": "UNIVERSITAS ISLAM NEGERI RADEN FATAH PALEMBANG",
    # Batusangkar
    "IAIN BATUSANGKAR": "UNIVERSITAS ISLAM NEGERI MAHMUD YUNUS BATUSANGKAR",
    "STAIN BATUSANGKAR": "UNIVERSITAS ISLAM NEGERI MAHMUD YUNUS BATUSANGKAR",
    # Langsa
    "STAIN LANGSA": "IAIN LANGSA",
}


# ── Helper Functions ─────────────────────────────────────────────────────────

def normalize_pt_name(pt_name: str) -> str:
    """Kembalikan nama kanonik PT (nama terbaru/resmi)."""
    key = pt_name.strip().upper()
    for alias, canonical in PT_NAME_ALIASES.items():
        if alias.upper() == key:
            return canonical
    return pt_name.strip()


def normalize_prodi_name(api_name: str) -> Optional[str]:
    upper = api_name.strip().replace('\xa0', ' ').upper()
    sorted_keys = sorted(PRODI_NORMALIZATION.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key in upper:
            return PRODI_NORMALIZATION[key]
    return None


def normalize_for_search(name: str) -> str:
    name = name.replace('\xa0', ' ')
    if "/" in name:
        name = name.split("/")[0].strip()
    name = re.sub(r'\(.*?\)', '', name).strip()
    return name


def get_rumpun_search_terms(rumpun: str) -> list[str]:
    """Bangun daftar kata kunci pencarian untuk discovery prodi satu rumpun."""
    terms = {rumpun.strip().upper()}

    for alias, normalized in PRODI_NORMALIZATION.items():
        if normalized == rumpun:
            terms.add(alias.strip().upper())

    if rumpun in PRODI_SEARCH_VARIANTS:
        for alias in PRODI_SEARCH_VARIANTS[rumpun]:
            terms.add(alias.strip().upper())

    return sorted(terms, key=len, reverse=True)


# ── WebSocket & Log Helpers ──────────────────────────────────────────────────

async def send_log(job_id: int, level: str, message: str, db: AsyncSession):
    """Simpan log ke DB dan broadcast via WebSocket."""
    log = ScrapeLog(job_id=job_id, level=level, message=message)
    db.add(log)
    await db.commit()

    from routers.scrape_router import broadcast_to_job
    await broadcast_to_job(job_id, {
        "type": "log",
        "level": level,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


async def send_progress(job_id: int, data: dict):
    """Broadcast progress update via WebSocket."""
    from routers.scrape_router import broadcast_to_job
    data["type"] = "progress"
    await broadcast_to_job(job_id, data)


# ── Main Scraping Job ────────────────────────────────────────────────────────

async def run_scraping_job(
    job_id: int,
    prodi_filter: List[str],
    semesters: Optional[List[str]] = None,
    pt_filter: Optional[str] = None,
):
    """
    Entry point scraping job — berjalan di background.
    prodi_filter : list rumpun prodi yang akan discrape
    semesters    : list kode semester (default: semua semester tersedia)
    pt_filter    : filter nama PT (opsional)
    """
    semesters = semesters or DEFAULT_SEMESTERS

    async with AsyncSessionLocal() as db:
        try:
            # ── Cek status API sebelum mulai ──────────────────────────────
            await send_log(job_id, "info", "🔍 Mengecek status API PDDikti...", db)
            api_status = await client.check_api_status()
            state = api_status.get("state", "unknown")
            if state == "limited":
                await send_log(
                    job_id, "warning",
                    f"⚠️ API PDDikti sedang dalam mode LIMITED (high traffic). "
                    f"Menggunakan endpoint HA: pddikti.fastapicloud.dev sebagai fallback.",
                    db
                )
            else:
                await send_log(job_id, "info", f"✅ Status API: {state}", db)

            await send_log(
                job_id, "info",
                f"🚀 Memulai scraping untuk {len(prodi_filter)} program studi...",
                db
            )

            # ══════════════════════════════════════════════════════════════
            # STEP 1: Discovery — cari prodi ID per rumpun
            # ══════════════════════════════════════════════════════════════
            await send_log(job_id, "info", "🌏 STEP 1: Discovery prodi nasional dari API PDDikti v4.1.0...", db)

            resolved = []
            seen_ids = set()

            for rumpun_idx, rumpun in enumerate(prodi_filter, 1):
                queries = get_rumpun_search_terms(rumpun)
                before_count = len(resolved)

                await send_log(
                    job_id, "info",
                    f"🔎 [{rumpun_idx}/{len(prodi_filter)}] Discovery '{rumpun}' "
                    f"dengan {len(queries)} kata kunci...",
                    db
                )

                for q_idx, query in enumerate(queries, 1):
                    await asyncio.sleep(REQ_DELAY)
                    results = await client.search_prodi(query)

                    for item in results:
                        if not isinstance(item, dict):
                            continue

                        pid      = str(item.get("id") or "").strip()
                        nama     = str(item.get("nama") or "").strip()
                        pt_name  = str(item.get("pt") or "").strip()
                        jenjang  = str(item.get("jenjang") or "").strip()
                        pt_singkat = str(item.get("pt_singkat") or "").strip()
                        id_sp    = str(item.get("id_sp") or "").strip()

                        if not pid or not nama or not pt_name:
                            continue
                        if pid in seen_ids:
                            continue

                        normalized = normalize_prodi_name(nama)
                        if normalized != rumpun:
                            continue

                        if pt_filter and pt_filter.strip().upper() not in pt_name.upper():
                            continue

                        seen_ids.add(pid)
                        canonical_pt = normalize_pt_name(pt_name)

                        resolved.append({
                            "id":           pid,
                            "id_sp":        id_sp,       # PT ID dari API
                            "nama":         nama,
                            "jenjang":      jenjang,
                            "pt":           canonical_pt,
                            "pt_singkat":   pt_singkat,
                            "rumpun":       rumpun,
                        })

                    if q_idx % 5 == 0 or q_idx == len(queries):
                        await send_progress(job_id, {
                            "phase":    "resolve",
                            "current":  q_idx,
                            "total":    len(queries),
                            "resolved": len(resolved),
                        })

                discovered = len(resolved) - before_count
                await send_log(
                    job_id, "info",
                    f"✅ '{rumpun}': {discovered} prodi ditemukan",
                    db
                )

                await db.execute(
                    update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                        total_prodi=len(resolved),
                        resolved_prodi=len(resolved),
                    )
                )
                await db.commit()

            await send_log(
                job_id, "success",
                f"✅ STEP 1 selesai: {len(resolved)} prodi unik ditemukan",
                db
            )

            if not resolved:
                await send_log(job_id, "warning", "⚠️ Tidak ada prodi yang ditemukan.", db)
                await db.execute(
                    update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                        status="completed",
                        completed_at=datetime.now(timezone.utc),
                        total_prodi=0, resolved_prodi=0, total_dosen=0, new_dosen=0,
                    )
                )
                await db.commit()
                return

            # ══════════════════════════════════════════════════════════════
            # STEP 2: Prodi Detail + PT Info
            # ══════════════════════════════════════════════════════════════
            await send_log(
                job_id, "info",
                f"📋 STEP 2: Mengambil detail {len(resolved)} prodi & info PT...",
                db
            )

            pt_detail_cache: dict[str, dict] = {}   # pt_name_upper → detail dict
            pt_id_cache: dict[str, str] = {}         # pt_name_upper → pddikti pt_id
            prodi_saved_count = 0

            for i, prodi in enumerate(resolved, 1):
                prodi_id   = prodi["id"]
                prodi_nama = prodi["nama"]
                pt_name    = prodi["pt"]
                rumpun     = prodi["rumpun"]

                # Skip jika sudah ada di DB
                existing_pd = await db.execute(
                    select(ProdiDetail).where(ProdiDetail.pddikti_id == prodi_id)
                )
                if existing_pd.scalar_one_or_none():
                    continue

                # ── Fetch prodi detail ────────────────────────────────────
                await asyncio.sleep(REQ_DELAY)
                raw_prodi_detail = await client.get_prodi_detail(prodi_id)
                if raw_prodi_detail and isinstance(raw_prodi_detail, dict):
                    prodi_detail = normalize_prodi_detail(raw_prodi_detail)
                else:
                    prodi_detail = {
                        "kode_prodi": "", "status": "Aktif", "akreditasi": "",
                        "status_akreditasi": "", "provinsi": "",
                        "id_sp": prodi.get("id_sp", ""),
                    }

                kode_prodi          = prodi_detail.get("kode_prodi", "")
                keterangan          = prodi_detail.get("status", "Aktif")
                akreditasi_val      = prodi_detail.get("akreditasi", "")
                status_akreditasi_v = prodi_detail.get("status_akreditasi", "")
                provinsi_prodi      = prodi_detail.get("provinsi", "")

                # Gunakan id_sp dari detail prodi jika tidak ada di resolved
                pt_sp_id = prodi_detail.get("id_sp", "") or prodi.get("id_sp", "")

                # ── Fetch PT detail (cached per PT name) ─────────────────
                pt_cache_key = pt_name.upper()
                pt_info = pt_detail_cache.get(pt_cache_key)

                if pt_info is None and pt_sp_id:
                    await asyncio.sleep(REQ_DELAY)
                    raw_pt_detail = await client.get_pt_detail(pt_sp_id)
                    if raw_pt_detail and isinstance(raw_pt_detail, dict):
                        pt_info = normalize_pt_detail(raw_pt_detail)
                        # Simpan PT ID untuk dipakai nanti fetch riwayat
                        pt_id_cache[pt_cache_key] = pt_sp_id

                        # ── Fetch riwayat nama PT (untuk alias STAIN/IAIN/UIN) ──
                        # Ini FITUR BARU dari API v4.1.0 — sangat membantu!
                        await asyncio.sleep(REQ_DELAY)
                        riwayat_pt = await client.get_pt_riwayat(pt_sp_id)
                        if riwayat_pt:
                            # Catat semua nama historis agar alias-nya dikenal
                            for rw in riwayat_pt:
                                if isinstance(rw, dict):
                                    nama_lama = str(rw.get("nama_pt") or rw.get("nama") or "").strip().upper()
                                    if nama_lama and nama_lama not in PT_NAME_ALIASES:
                                        PT_NAME_ALIASES[nama_lama] = pt_name
                    else:
                        pt_info = {}
                    pt_detail_cache[pt_cache_key] = pt_info

                elif pt_info is None:
                    pt_info = {}
                    pt_detail_cache[pt_cache_key] = pt_info

                # ── Klasifikasi PTN/PTS dan PTKIN/NON-PTKIN ──────────────
                status_pt_raw = pt_info.get("status_pt", "").upper()
                kelompok_raw  = pt_info.get("kelompok", "").upper()
                pembina_raw   = pt_info.get("pembina", "").upper()

                ptn_pts = "PTN" if ("NEGERI" in status_pt_raw or status_pt_raw == "PTN") else "PTS"
                ptkin = (
                    "PTKIN"
                    if ("PTKIN" in kelompok_raw or "AGAMA" in pembina_raw or "KEMENAG" in pembina_raw)
                    else "NON PTKIN"
                )
                dikti = "DIKTIS" if ptkin == "PTKIN" else "DIKTI"
                provinsi_final = pt_info.get("provinsi", "") or provinsi_prodi

                # ── Hitung jumlah dosen (scan semua semester) ─────────────
                jumlah_dosen = 0
                counted_keys: set[str] = set()
                latest_sem = None

                for sem in semesters:
                    for mode in DOSEN_FETCH_MODES:
                        await asyncio.sleep(REQ_DELAY)
                        if mode == "homebase":
                            raw_list = await client.get_dosen_homebase(prodi_id, sem)
                        else:
                            raw_list = await client.get_dosen_penghitung_ratio(prodi_id, sem)

                        if raw_list and isinstance(raw_list, list) and len(raw_list) > 0:
                            if latest_sem is None:
                                latest_sem = sem
                            for d in raw_list:
                                nidn  = str(d.get("nidn") or "").strip()
                                nama  = str(d.get("nama_dosen") or d.get("nama") or "").strip()
                                key   = nidn if nidn else nama
                                if key:
                                    counted_keys.add(key)

                jumlah_dosen = len(counted_keys)
                if latest_sem:
                    prodi["semester_terakhir"] = latest_sem

                # ── Get or create PerguruanTinggi ─────────────────────────
                pt_result = await db.execute(
                    select(PerguruanTinggi).where(PerguruanTinggi.nama == pt_name)
                )
                pt_obj = pt_result.scalar_one_or_none()
                if not pt_obj:
                    pt_obj = PerguruanTinggi(
                        pddikti_id=pt_sp_id or None,
                        nama=pt_name,
                        provinsi=provinsi_final,
                        status_pt=ptn_pts,
                        kelompok=ptkin,
                        pembina=dikti,
                    )
                    db.add(pt_obj)
                    await db.flush()
                else:
                    if not pt_obj.status_pt:
                        pt_obj.status_pt = ptn_pts
                    if not pt_obj.kelompok:
                        pt_obj.kelompok = ptkin
                    if not pt_obj.pembina:
                        pt_obj.pembina = dikti
                    if not pt_obj.provinsi:
                        pt_obj.provinsi = provinsi_final
                    if not pt_obj.pddikti_id and pt_sp_id:
                        pt_obj.pddikti_id = pt_sp_id

                # ── Simpan ProdiDetail ────────────────────────────────────
                pd_obj = ProdiDetail(
                    pddikti_id=prodi_id,
                    pt_id=pt_obj.id,
                    nama_prodi=prodi_nama,
                    rumpun=rumpun,
                    jenjang=prodi.get("jenjang", ""),
                    kode_prodi=kode_prodi,
                    jumlah_dosen=jumlah_dosen,
                    keterangan=keterangan,
                    akreditasi=akreditasi_val,
                    status_akreditasi=status_akreditasi_v or (
                        "Belum Terakreditasi" if not akreditasi_val else "Terakreditasi"
                    ),
                    ptn_pts=ptn_pts,
                    ptkin_non_ptkin=ptkin,
                    dikti_diktis=dikti,
                    provinsi=provinsi_final,
                    semester_terakhir=prodi.get("semester_terakhir", ""),
                )
                db.add(pd_obj)
                prodi_saved_count += 1

                if prodi_saved_count % 25 == 0:
                    await db.commit()
                    await send_log(
                        job_id, "info",
                        f"📋 Detail prodi tersimpan: {prodi_saved_count}/{len(resolved)}",
                        db
                    )

                if i % 10 == 0 or i <= 3 or i == len(resolved):
                    await send_progress(job_id, {
                        "phase":   "prodi_detail",
                        "current": i,
                        "total":   len(resolved),
                        "saved":   prodi_saved_count,
                    })
                    await db.execute(
                        update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                            total_prodi_detail=prodi_saved_count,
                            new_prodi_detail=prodi_saved_count,
                        )
                    )
                    await db.commit()

            await db.commit()
            await send_log(
                job_id, "success",
                f"✅ STEP 2 selesai: {prodi_saved_count} detail prodi tersimpan",
                db
            )

            # ══════════════════════════════════════════════════════════════
            # STEP 3: Collect Dosen (homebase + penghitung-ratio) per semester
            # ══════════════════════════════════════════════════════════════
            await send_log(
                job_id, "info",
                f"📚 STEP 3: Mengambil dosen dari {len(resolved)} prodi "
                f"(homebase + penghitung-ratio, scan semua semester)...",
                db
            )

            all_dosen: list[dict] = []
            seen_global: set[str] = set()
            new_count  = 0
            skip_count = 0

            def _collect_dosen_batch(
                raw_list: list,
                prodi: dict,
                rumpun: str,
                mode: str,
                seen: set,
                result_list: list,
            ) -> int:
                """
                Parse dan deduplikasi satu batch dosen dari API.
                Deduplication key: NIDN > NUPTK > nama+PT
                Dosen tidak aktif tetap dikumpulkan (tidak difilter).
                """
                added = 0
                for raw in raw_list:
                    if not isinstance(raw, dict):
                        continue

                    norm = normalize_dosen_from_list(raw)
                    nidn       = norm["nidn"]
                    nuptk      = norm["nuptk"]
                    nama       = norm["nama_dosen"]
                    pddikti_id = norm["id"]

                    if not nama:
                        continue

                    # PT homebase dosen (bisa berbeda dari PT pemilik prodi)
                    pt_dosen_raw = norm["nama_pt"] or prodi["pt"]
                    pt_dosen = normalize_pt_name(pt_dosen_raw)

                    # Deduplication key
                    if nidn:
                        key = f"nidn:{nidn}"
                    elif nuptk:
                        key = f"nuptk:{nuptk}"
                    elif pddikti_id:
                        key = f"id:{pddikti_id}"
                    else:
                        key = f"nama:{nama.upper()}|pt:{pt_dosen.upper()}"

                    if key in seen:
                        continue
                    seen.add(key)

                    # Cross-lock alias keys
                    if nidn:
                        seen.add(f"nidn:{nidn}")
                    if nuptk:
                        seen.add(f"nuptk:{nuptk}")
                    if pddikti_id:
                        seen.add(f"id:{pddikti_id}")

                    added += 1
                    result_list.append({
                        "pddikti_id":    pddikti_id,
                        "nidn":          nidn,
                        "nuptk":         nuptk,
                        "nama_dosen":    nama,
                        "jenis_kelamin": norm["jenis_kelamin"],
                        "pendidikan":    norm["pendidikan"],
                        "jabatan_fungsional": norm["jabatan_fungsional"],
                        "status_aktif":  norm["status_aktif"],
                        "status_pegawai": norm["status_pegawai"],
                        "ikatan_kerja":  norm["ikatan_kerja"],
                        "rumpun_prodi":  rumpun,
                        "prodi_api":     prodi["nama"],
                        "jenjang_prodi": prodi["jenjang"],
                        "pt_asal":       normalize_pt_name(prodi["pt"]),
                        "pt_dosen":      pt_dosen,
                        "sumber":        mode,
                    })
                return added

            for i, prodi in enumerate(resolved, 1):
                prodi_id    = prodi["id"]
                rumpun      = prodi["rumpun"]
                prodi_label = f"{prodi['nama']} ({prodi['jenjang']}) - {prodi['pt']}"
                prodi_new   = 0

                # ── Lapis 1: tanpa semester parameter ─────────────────────
                # API baru: kita scan dengan semester terbaru dulu
                for mode in DOSEN_FETCH_MODES:
                    await asyncio.sleep(REQ_DELAY)
                    if mode == "homebase":
                        raw_list = await client.get_dosen_homebase(prodi_id, DEFAULT_SEMESTERS[0])
                    else:
                        raw_list = await client.get_dosen_penghitung_ratio(prodi_id, DEFAULT_SEMESTERS[0])

                    if raw_list:
                        added = _collect_dosen_batch(raw_list, prodi, rumpun, mode, seen_global, all_dosen)
                        prodi_new += added
                        new_count += added

                # ── Lapis 2: scan SEMUA semester (selalu dijalankan) ───────
                # Dosen di semester berbeda bisa tidak muncul di semester terbaru.
                for sem in semesters[1:]:  # mulai dari index 1 (terbaru sudah di Lapis 1)
                    for mode in DOSEN_FETCH_MODES:
                        await asyncio.sleep(REQ_DELAY)
                        if mode == "homebase":
                            raw_list = await client.get_dosen_homebase(prodi_id, sem)
                        else:
                            raw_list = await client.get_dosen_penghitung_ratio(prodi_id, sem)

                        if raw_list:
                            added = _collect_dosen_batch(raw_list, prodi, rumpun, mode, seen_global, all_dosen)
                            prodi_new += added
                            new_count += added

                if i % 10 == 0 or i <= 3 or i == len(resolved):
                    await send_log(
                        job_id, "info",
                        f"📊 [{i}/{len(resolved)}] {prodi_label} → "
                        f"{prodi_new} baru | Total: {len(all_dosen)}",
                        db
                    )
                    await send_progress(job_id, {
                        "phase":         "fetch",
                        "current":       i,
                        "total":         len(resolved),
                        "total_dosen":   len(all_dosen),
                        "new_dosen":     new_count,
                        "skipped_dosen": skip_count,
                    })
                    await db.execute(
                        update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                            total_dosen=len(all_dosen),
                            new_dosen=new_count,
                            skipped_dosen=skip_count,
                        )
                    )
                    await db.commit()

            await send_log(
                job_id, "success",
                f"✅ STEP 3 selesai: {len(all_dosen)} dosen unik ditemukan",
                db
            )

            # ══════════════════════════════════════════════════════════════
            # STEP 4: Fetch profil detail + simpan ke DB
            # ══════════════════════════════════════════════════════════════
            await send_log(job_id, "info", f"💾 STEP 4: Fetching profil & menyimpan {len(all_dosen)} dosen...", db)

            async def _fetch_dosen_profile(
                pddikti_id: str,
                nidn: str,
                nuptk: str,
                nama: str,
                pt_name: str,
                pt_dosen_name: str,
            ) -> Optional[dict]:
                """
                Fetch profil dosen dengan strategi berlapis:
                  A — via pddikti_id langsung (API baru, paling cepat & akurat)
                  B — search via NIDN → ambil id → fetch profil
                  C — search via NUPTK → ambil id → fetch profil
                  D — search via Nama → match PT → fetch profil
                """
                # Lapis A: langsung gunakan pddikti_id (tersedia di API baru)
                if pddikti_id:
                    await asyncio.sleep(REQ_DELAY)
                    prof = await client.get_dosen_profile(pddikti_id)
                    if prof and isinstance(prof, dict):
                        return normalize_dosen_profile(prof)

                # Lapis B: search via NIDN
                async def _search_and_get(keyword: str) -> Optional[dict]:
                    await asyncio.sleep(REQ_DELAY)
                    results = await client.search_dosen(keyword)
                    if not results or not isinstance(results, list):
                        return None

                    sid = None
                    for sr in results:
                        sr_nidn  = str(sr.get("nidn",    "") or "").strip()
                        sr_nuptk = str(sr.get("nuptk",   "") or "").strip()
                        sr_nama  = str(sr.get("nama",    "") or "").upper().strip()
                        sr_pt    = str(sr.get("nama_pt", "") or sr.get("pt", "") or "").upper().strip()

                        if nidn and sr_nidn and sr_nidn == nidn:
                            sid = sr.get("id", "")
                            break
                        if nuptk and sr_nuptk and sr_nuptk == nuptk:
                            sid = sr.get("id", "")
                            break
                        if sr_nama == nama.upper():
                            if (pt_name.upper() in sr_pt or sr_pt in pt_name.upper()
                                    or pt_dosen_name.upper() in sr_pt):
                                sid = sr.get("id", "")
                                break

                    if not sid and results:
                        sid = str(results[0].get("id", "") or "")

                    if sid:
                        await asyncio.sleep(REQ_DELAY)
                        prof = await client.get_dosen_profile(sid)
                        if prof and isinstance(prof, dict):
                            return normalize_dosen_profile(prof)
                    return None

                if nidn:
                    result = await _search_and_get(nidn)
                    if result:
                        return result

                if nuptk:
                    result = await _search_and_get(nuptk)
                    if result:
                        return result

                if nama:
                    result = await _search_and_get(nama)
                    if result:
                        return result

                return None

            saved_count = 0

            for i, dosen_data in enumerate(all_dosen, 1):
                pddikti_id    = dosen_data["pddikti_id"]
                nidn          = dosen_data["nidn"]
                nuptk         = dosen_data.get("nuptk", "")
                nama          = dosen_data["nama_dosen"]
                pt_name       = dosen_data["pt_asal"]
                pt_dosen_name = dosen_data.get("pt_dosen", pt_name)
                rumpun        = dosen_data["rumpun_prodi"]

                # ── Cek duplikat DB berlapis (pddikti_id → NIDN → NUPTK → Nama+PT) ──
                existing_dosen = None

                if pddikti_id:
                    res = await db.execute(select(Dosen).where(Dosen.pddikti_id == pddikti_id))
                    existing_dosen = res.scalar_one_or_none()

                if not existing_dosen and nidn:
                    res = await db.execute(select(Dosen).where(Dosen.nidn == nidn))
                    existing_dosen = res.scalar_one_or_none()

                if not existing_dosen and nuptk:
                    res = await db.execute(select(Dosen).where(Dosen.nuptk == nuptk))
                    existing_dosen = res.scalar_one_or_none()

                if not existing_dosen:
                    pt_res = await db.execute(
                        select(PerguruanTinggi).where(
                            PerguruanTinggi.nama.in_(list({pt_name, pt_dosen_name}))
                        )
                    )
                    pt_ids = [r.id for r in pt_res.scalars().all()]
                    if pt_ids:
                        res = await db.execute(
                            select(Dosen).where(Dosen.nama == nama, Dosen.pt_id.in_(pt_ids))
                        )
                        existing_dosen = res.scalar_one_or_none()

                if existing_dosen:
                    # Update identifier yang kosong
                    updated = False
                    if pddikti_id and not existing_dosen.pddikti_id:
                        existing_dosen.pddikti_id = pddikti_id; updated = True
                    if nidn and not existing_dosen.nidn:
                        existing_dosen.nidn = nidn; updated = True
                    if nuptk and not existing_dosen.nuptk:
                        existing_dosen.nuptk = nuptk; updated = True
                    if updated:
                        await db.flush()
                    skip_count += 1
                    continue

                # ── Fetch profil detail ───────────────────────────────────
                profile = await _fetch_dosen_profile(
                    pddikti_id, nidn, nuptk, nama, pt_name, pt_dosen_name
                )

                # Update NIDN/NUPTK/ID dari profil jika sebelumnya kosong
                if profile:
                    if not nidn and profile.get("nidn"):
                        nidn = profile["nidn"]
                        dosen_data["nidn"] = nidn
                    if not nuptk and profile.get("nuptk"):
                        nuptk = profile["nuptk"]
                        dosen_data["nuptk"] = nuptk
                    if not pddikti_id and profile.get("id"):
                        pddikti_id = profile["id"]

                # ── Re-check duplikat setelah profil ditemukan ───────────
                recheck = None
                if pddikti_id:
                    res = await db.execute(select(Dosen).where(Dosen.pddikti_id == pddikti_id))
                    recheck = res.scalar_one_or_none()
                if not recheck and nidn:
                    res = await db.execute(select(Dosen).where(Dosen.nidn == nidn))
                    recheck = res.scalar_one_or_none()
                if not recheck and nuptk:
                    res = await db.execute(select(Dosen).where(Dosen.nuptk == nuptk))
                    recheck = res.scalar_one_or_none()

                if recheck:
                    if pddikti_id and not recheck.pddikti_id:
                        recheck.pddikti_id = pddikti_id; await db.flush()
                    if nidn and not recheck.nidn:
                        recheck.nidn = nidn; await db.flush()
                    if nuptk and not recheck.nuptk:
                        recheck.nuptk = nuptk; await db.flush()
                    skip_count += 1
                    continue

                # ── Get or create PT ──────────────────────────────────────
                pt_result = await db.execute(
                    select(PerguruanTinggi).where(PerguruanTinggi.nama == pt_name)
                )
                pt_obj = pt_result.scalar_one_or_none()
                if not pt_obj:
                    pt_obj = PerguruanTinggi(nama=pt_name)
                    db.add(pt_obj)
                    await db.flush()

                # ── Get or create ProgramStudi ────────────────────────────
                prodi_api_name = dosen_data["prodi_api"]
                prodi_result = await db.execute(
                    select(ProgramStudi).where(
                        ProgramStudi.nama == prodi_api_name,
                        ProgramStudi.pt_id == pt_obj.id,
                    )
                )
                prodi_obj = prodi_result.scalar_one_or_none()
                if not prodi_obj:
                    prodi_obj = ProgramStudi(
                        nama=prodi_api_name,
                        rumpun=rumpun,
                        jenjang=dosen_data["jenjang_prodi"],
                        pt_id=pt_obj.id,
                    )
                    db.add(prodi_obj)
                    await db.flush()

                # ── Buat record Dosen ─────────────────────────────────────
                dosen_obj = Dosen(
                    pddikti_id=pddikti_id or None,
                    nidn=nidn or None,
                    nuptk=nuptk or None,
                    nama=(profile.get("nama_dosen") if profile else None) or nama,
                    jenis_kelamin=(profile.get("jenis_kelamin") if profile else None)
                                  or dosen_data.get("jenis_kelamin", ""),
                    jabatan_fungsional=(profile.get("jabatan_akademik") if profile else None)
                                       or dosen_data.get("jabatan_fungsional", ""),
                    pendidikan_terakhir=(profile.get("pendidikan_tertinggi") if profile else None)
                                        or dosen_data.get("pendidikan", ""),
                    status_ikatan_kerja=(profile.get("status_ikatan_kerja") if profile else None)
                                        or dosen_data.get("ikatan_kerja", ""),
                    status_aktivitas=(profile.get("status_aktivitas") if profile else None)
                                     or dosen_data.get("status_aktif", ""),
                    pt_id=pt_obj.id,
                    prodi_id=prodi_obj.id,
                    rumpun_prodi=rumpun,
                    jenjang_prodi=dosen_data["jenjang_prodi"],
                )
                db.add(dosen_obj)
                saved_count += 1

                if saved_count % 50 == 0:
                    await db.commit()
                    await send_log(
                        job_id, "info",
                        f"💾 Tersimpan: {saved_count}/{len(all_dosen)} dosen",
                        db
                    )
                    await send_progress(job_id, {
                        "phase":   "save",
                        "current": i,
                        "total":   len(all_dosen),
                        "saved":   saved_count,
                    })

            await db.commit()

            # ── Job Complete ──────────────────────────────────────────────
            elapsed = ""
            job_result = await db.execute(select(ScrapeJob).where(ScrapeJob.id == job_id))
            job_obj = job_result.scalar_one_or_none()
            if job_obj and job_obj.started_at:
                diff = datetime.now(timezone.utc) - job_obj.started_at.replace(tzinfo=timezone.utc)
                minutes = int(diff.total_seconds() // 60)
                seconds = int(diff.total_seconds() % 60)
                elapsed = f"{minutes}m{seconds}s"

            await db.execute(
                update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                    status="completed",
                    completed_at=datetime.now(timezone.utc),
                    total_dosen=saved_count,
                    new_dosen=saved_count,
                    skipped_dosen=skip_count,
                    total_prodi_detail=prodi_saved_count,
                    new_prodi_detail=prodi_saved_count,
                )
            )
            await db.commit()

            await send_log(
                job_id, "success",
                f"🎉 Scraping selesai! {saved_count} dosen + {prodi_saved_count} prodi tersimpan ({elapsed})",
                db
            )

            from routers.scrape_router import broadcast_to_job
            await broadcast_to_job(job_id, {
                "type":              "done",
                "total_dosen":       saved_count,
                "total_prodi_detail": prodi_saved_count,
                "new":               saved_count,
                "skipped":           skip_count,
                "elapsed":           elapsed,
            })

        except asyncio.CancelledError:
            await send_log(job_id, "warning", "⚠️ Scraping dibatalkan oleh user", db)
            await db.execute(
                update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                    status="cancelled",
                    completed_at=datetime.now(timezone.utc),
                    error_message="Dibatalkan",
                )
            )
            await db.commit()

        except Exception as e:
            import traceback
            error_msg = str(e)
            tb = traceback.format_exc()
            await send_log(job_id, "error", f"❌ Error: {error_msg}\n{tb[:500]}", db)
            await _fail_job(db, job_id, error_msg)


async def _fail_job(db: AsyncSession, job_id: int, error_msg: str):
    await db.execute(
        update(ScrapeJob).where(ScrapeJob.id == job_id).values(
            status="failed",
            completed_at=datetime.now(timezone.utc),
            error_message=error_msg,
        )
    )
    await db.commit()

    from routers.scrape_router import broadcast_to_job
    await broadcast_to_job(job_id, {
        "type":    "error",
        "message": error_msg,
    })
