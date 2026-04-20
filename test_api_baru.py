"""
Test script untuk validasi API PDDikti v4.1.0
================================================
Jalankan: python test_api_baru.py
"""

import asyncio
import sys
sys.stdout.reconfigure(encoding='utf-8')  # Windows compat

import services.pddikti_client as client
from services.pddikti_client import (
    normalize_dosen_from_list,
    normalize_dosen_profile,
    normalize_prodi_detail,
    normalize_pt_detail,
)


async def main():
    print("=" * 60)
    print("TEST API PDDIKTI v4.1.0 (pddikti.rone.dev)")
    print("=" * 60)

    # ── 1. Cek Status API ────────────────────────────────────────
    print("\n[1] Cek Status API...")
    status = await client.check_api_status()
    print(f"    State    : {status.get('state', '?')}")
    print(f"    Available: {status.get('available', '?')}")
    print(f"    Message  : {status.get('message', '')[:80]}")

    if not status.get("available"):
        print("    [STOP] API tidak tersedia. Test dihentikan.")
        return

    # ── 2. Search Prodi ──────────────────────────────────────────
    print("\n[2] Search Prodi 'perbankan syariah'...")
    prodi_list = await client.search_prodi("perbankan syariah")
    print(f"    Jumlah hasil: {len(prodi_list)}")
    if prodi_list:
        p = prodi_list[0]
        print(f"    Contoh: {p.get('nama')} | {p.get('pt')} | ID: {p.get('id')}")
        prodi_id = p.get("id")
        pt_id    = p.get("id_sp")
    else:
        print("    [SKIP] Tidak ada hasil search prodi.")
        prodi_id = None
        pt_id = None

    # ── 3. Detail Prodi ──────────────────────────────────────────
    if prodi_id:
        print(f"\n[3] Detail Prodi ID: {prodi_id}")
        raw = await client.get_prodi_detail(prodi_id)
        if raw:
            detail = normalize_prodi_detail(raw)
            print(f"    Nama     : {detail.get('nama_prodi')}")
            print(f"    Status   : {detail.get('status')}")
            print(f"    Akreditasi: {detail.get('akreditasi')}")
            print(f"    PT ID    : {detail.get('id_sp')}")
            if not pt_id:
                pt_id = detail.get("id_sp")
        else:
            print("    [WARN] Detail prodi tidak berhasil diambil.")

    # ── 4. Detail PT ─────────────────────────────────────────────
    if pt_id:
        print(f"\n[4] Detail PT ID: {pt_id}")
        raw = await client.get_pt_detail(pt_id)
        if raw:
            detail = normalize_pt_detail(raw)
            print(f"    Nama     : {detail.get('nama')}")
            print(f"    Status PT: {detail.get('status_pt')}")
            print(f"    Kelompok : {detail.get('kelompok')}")
            print(f"    Provinsi : {detail.get('provinsi')}")
        else:
            print("    [WARN] Detail PT tidak berhasil diambil.")

        # ── 5. Riwayat PT ─────────────────────────────────────────
        print(f"\n[5] Riwayat Nama PT ID: {pt_id}")
        riwayat = await client.get_pt_riwayat(pt_id)
        print(f"    Jumlah riwayat: {len(riwayat)}")
        for r in riwayat[:3]:
            print(f"    - {r}")
    else:
        print("\n[4,5] Tidak ada PT ID, skip.")

    # ── 6. Dosen Homebase ────────────────────────────────────────
    if prodi_id:
        semester = "20242"
        print(f"\n[6] Dosen Homebase prodi {prodi_id} semester {semester}...")
        dosen_list = await client.get_dosen_homebase(prodi_id, semester)
        print(f"    Jumlah dosen: {len(dosen_list)}")
        if dosen_list:
            d = normalize_dosen_from_list(dosen_list[0])
            print(f"    Contoh: {d.get('nama_dosen')} | NIDN: {d.get('nidn')} | ID: {d.get('id')}")
            dosen_id = d.get("id")
        else:
            print("    [SKIP] Tidak ada dosen di semester ini.")
            dosen_id = None

        # ── 7. Penghitung Ratio ────────────────────────────────────
        print(f"\n[7] Dosen Penghitung Ratio prodi {prodi_id} semester {semester}...")
        ratio_list = await client.get_dosen_penghitung_ratio(prodi_id, semester)
        print(f"    Jumlah dosen: {len(ratio_list)}")

        # ── 8. Profil Dosen ────────────────────────────────────────
        if dosen_list and dosen_id:
            print(f"\n[8] Profil Dosen ID: {dosen_id}")
            raw = await client.get_dosen_profile(dosen_id)
            if raw:
                prof = normalize_dosen_profile(raw)
                print(f"    Nama     : {prof.get('nama_dosen')}")
                print(f"    NIDN     : {prof.get('nidn')}")
                print(f"    Jabatan  : {prof.get('jabatan_akademik')}")
                print(f"    Pendidikan: {prof.get('pendidikan_tertinggi')}")
                print(f"    Status   : {prof.get('status_aktivitas')}")
                print(f"    PT       : {prof.get('nama_pt')}")
            else:
                print("    [WARN] Profil dosen tidak berhasil diambil.")

    # ── 9. Search Dosen ──────────────────────────────────────────
    print("\n[9] Search Dosen 'ahmad'...")
    dosen_search = await client.search_dosen("ahmad")
    print(f"    Jumlah hasil: {len(dosen_search)}")
    if dosen_search:
        ds = dosen_search[0]
        print(f"    Contoh: {ds.get('nama') or ds.get('nama_dosen')} | NIDN: {ds.get('nidn')}")

    # ── 10. Prodi Bidang Ilmu ─────────────────────────────────────
    print("\n[10] Prodi bidang ilmu 'agama'...")
    agama_list = await client.get_prodi_by_bidang_ilmu("agama")
    print(f"    Jumlah prodi bidang agama: {len(agama_list)}")
    if agama_list:
        ag = agama_list[0]
        print(f"    Contoh: {ag.get('nama') or ag.get('nama_prodi')} | {ag.get('pt')}")

    print("\n" + "=" * 60)
    print("TEST SELESAI")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
