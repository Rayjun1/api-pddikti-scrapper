"""
PDDikti Dosen Explorer — Prodi Detail Router
List, filter, stats, and export scraped program studi data.
"""

import io
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, or_, and_, desc, asc, delete
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, get_current_user_for_download, log_activity
from database import get_db
from models import ProdiDetail, PerguruanTinggi, ScrapeJob, User

router = APIRouter(prefix="/api/prodi-detail", tags=["prodi-detail"])


# ── List Prodi Detail ──
@router.get("")
async def list_prodi_detail(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    rumpun: Optional[List[str]] = Query(None),
    jenjang: Optional[str] = Query(None),
    akreditasi: Optional[str] = Query(None),
    ptn_pts: Optional[str] = Query(None),
    ptkin: Optional[str] = Query(None, alias="ptkin_non_ptkin"),
    dikti: Optional[str] = Query(None, alias="dikti_diktis"),
    provinsi: Optional[str] = Query(None),
    keterangan: Optional[str] = Query(None),
    pt: Optional[str] = Query(None),
    sort_by: str = Query("nama_prodi"),
    sort_order: str = Query("asc"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List prodi detail with pagination, filtering, and sorting."""
    query = select(ProdiDetail)
    conditions = []

    if search:
        term = f"%{search}%"
        conditions.append(or_(
            ProdiDetail.nama_prodi.ilike(term),
            ProdiDetail.kode_prodi.ilike(term),
        ))
    if rumpun:
        conditions.append(ProdiDetail.rumpun.in_(rumpun))
    if jenjang:
        conditions.append(ProdiDetail.jenjang == jenjang)
    if akreditasi:
        conditions.append(ProdiDetail.akreditasi == akreditasi)
    if ptn_pts:
        conditions.append(ProdiDetail.ptn_pts == ptn_pts)
    if ptkin:
        conditions.append(ProdiDetail.ptkin_non_ptkin == ptkin)
    if dikti:
        conditions.append(ProdiDetail.dikti_diktis == dikti)
    if provinsi:
        conditions.append(ProdiDetail.provinsi.ilike(f"%{provinsi}%"))
    if keterangan:
        conditions.append(ProdiDetail.keterangan == keterangan)

    if conditions:
        query = query.where(and_(*conditions))

    if pt:
        query = query.join(PerguruanTinggi, ProdiDetail.pt_id == PerguruanTinggi.id).where(
            PerguruanTinggi.nama.ilike(f"%{pt}%")
        )

    # Count
    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar()

    # Sort
    sort_col = {
        "nama_prodi": ProdiDetail.nama_prodi,
        "jenjang": ProdiDetail.jenjang,
        "akreditasi": ProdiDetail.akreditasi,
        "jumlah_dosen": ProdiDetail.jumlah_dosen,
        "rumpun": ProdiDetail.rumpun,
        "provinsi": ProdiDetail.provinsi,
        "ptn_pts": ProdiDetail.ptn_pts,
    }.get(sort_by, ProdiDetail.nama_prodi)
    order = desc(sort_col) if sort_order == "desc" else asc(sort_col)
    query = query.order_by(order)

    # Paginate
    query = query.offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(query)
    items = result.scalars().all()

    # Get PT names
    pt_ids = list(set(p.pt_id for p in items if p.pt_id))
    pt_map = {}
    if pt_ids:
        pt_result = await db.execute(select(PerguruanTinggi).where(PerguruanTinggi.id.in_(pt_ids)))
        pt_map = {p.id: p.nama for p in pt_result.scalars().all()}

    return {
        "data": [
            {
                "id": p.id,
                "nama_prodi": p.nama_prodi,
                "jenjang": p.jenjang or "",
                "perguruan_tinggi": pt_map.get(p.pt_id, ""),
                "jumlah_dosen": p.jumlah_dosen or 0,
                "keterangan": p.keterangan or "",
                "akreditasi": p.akreditasi or "",
                "status_akreditasi": p.status_akreditasi or "",
                "ptn_pts": p.ptn_pts or "",
                "ptkin_non_ptkin": p.ptkin_non_ptkin or "",
                "dikti_diktis": p.dikti_diktis or "",
                "provinsi": p.provinsi or "",
                "rumpun": p.rumpun or "",
                "semester_terakhir": p.semester_terakhir or "",
            }
            for p in items
        ],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if total else 0,
        },
    }


# ── Filter options ──
@router.get("/filters")
async def get_prodi_filter_options(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    async def _group(col):
        q = await db.execute(
            select(col, func.count(ProdiDetail.id))
            .where(col.isnot(None), col != "")
            .group_by(col).order_by(desc(func.count(ProdiDetail.id)))
        )
        return [{"value": r[0], "count": r[1]} for r in q.all()]

    return {
        "rumpun": await _group(ProdiDetail.rumpun),
        "jenjang": await _group(ProdiDetail.jenjang),
        "akreditasi": await _group(ProdiDetail.akreditasi),
        "ptn_pts": await _group(ProdiDetail.ptn_pts),
        "ptkin_non_ptkin": await _group(ProdiDetail.ptkin_non_ptkin),
        "dikti_diktis": await _group(ProdiDetail.dikti_diktis),
        "keterangan": await _group(ProdiDetail.keterangan),
        "provinsi": await _group(ProdiDetail.provinsi),
    }


# ── Stats for dashboard ──
@router.get("/stats")
async def get_prodi_stats(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    total = (await db.execute(select(func.count()).select_from(ProdiDetail))).scalar() or 0

    # Unique PT count
    total_pt = (await db.execute(
        select(func.count(func.distinct(ProdiDetail.pt_id)))
    )).scalar() or 0

    async def _dist(col):
        q = await db.execute(
            select(col, func.count(ProdiDetail.id))
            .where(col.isnot(None), col != "")
            .group_by(col).order_by(desc(func.count(ProdiDetail.id)))
        )
        return {r[0]: r[1] for r in q.all()}

    by_rumpun = await _dist(ProdiDetail.rumpun)
    by_jenjang = await _dist(ProdiDetail.jenjang)
    by_akreditasi = await _dist(ProdiDetail.akreditasi)
    by_status_akreditasi = await _dist(ProdiDetail.status_akreditasi)
    by_ptn_pts = await _dist(ProdiDetail.ptn_pts)
    by_ptkin = await _dist(ProdiDetail.ptkin_non_ptkin)
    by_dikti = await _dist(ProdiDetail.dikti_diktis)
    by_keterangan = await _dist(ProdiDetail.keterangan)
    by_provinsi = await _dist(ProdiDetail.provinsi)

    # Akreditasi cross-tabulation: akreditasi × dikti_diktis
    akreditasi_cross_q = await db.execute(
        select(
            ProdiDetail.akreditasi,
            ProdiDetail.dikti_diktis,
            func.count(ProdiDetail.id),
        )
        .where(ProdiDetail.akreditasi.isnot(None))
        .group_by(ProdiDetail.akreditasi, ProdiDetail.dikti_diktis)
    )
    akreditasi_cross = {}
    for akr, dikt, cnt in akreditasi_cross_q.all():
        akreditasi_cross.setdefault(akr or "Belum Terakreditasi", {})[dikt or ""] = cnt

    # Akreditasi × PTN/PTS
    akr_ptn_q = await db.execute(
        select(
            ProdiDetail.akreditasi,
            ProdiDetail.ptn_pts,
            func.count(ProdiDetail.id),
        )
        .where(ProdiDetail.akreditasi.isnot(None))
        .group_by(ProdiDetail.akreditasi, ProdiDetail.ptn_pts)
    )
    akreditasi_ptn = {}
    for akr, ptn, cnt in akr_ptn_q.all():
        akreditasi_ptn.setdefault(akr or "Belum Terakreditasi", {})[ptn or ""] = cnt

    # Last scrape
    last_job = await db.execute(
        select(ScrapeJob)
        .where(ScrapeJob.status == "completed")
        .order_by(desc(ScrapeJob.completed_at)).limit(1)
    )
    last = last_job.scalar_one_or_none()

    return {
        "total_prodi": total,
        "total_pt": total_pt,
        "total_rumpun": len(by_rumpun),
        "by_rumpun": by_rumpun,
        "by_jenjang": by_jenjang,
        "by_akreditasi": by_akreditasi,
        "by_status_akreditasi": by_status_akreditasi,
        "by_ptn_pts": by_ptn_pts,
        "by_ptkin": by_ptkin,
        "by_dikti": by_dikti,
        "by_keterangan": by_keterangan,
        "by_provinsi": by_provinsi,
        "akreditasi_cross_dikti": akreditasi_cross,
        "akreditasi_cross_ptn": akreditasi_ptn,
        "last_scrape": last.completed_at.isoformat() if last and last.completed_at else None,
    }


# ── Export to Excel ──
@router.get("/export")
async def export_prodi_excel(
    request: Request,
    search: Optional[str] = Query(None),
    rumpun: Optional[List[str]] = Query(None),
    jenjang: Optional[str] = Query(None),
    akreditasi: Optional[str] = Query(None),
    ptn_pts: Optional[str] = Query(None),
    ptkin: Optional[str] = Query(None, alias="ptkin_non_ptkin"),
    dikti: Optional[str] = Query(None, alias="dikti_diktis"),
    provinsi: Optional[str] = Query(None),
    pt: Optional[str] = Query(None),
    user: User = Depends(get_current_user_for_download),
    db: AsyncSession = Depends(get_db),
):
    from services.exporter import export_prodi_detail_excel

    query = select(ProdiDetail)
    conditions = []

    if search:
        term = f"%{search}%"
        conditions.append(or_(ProdiDetail.nama_prodi.ilike(term), ProdiDetail.kode_prodi.ilike(term)))
    if rumpun:
        conditions.append(ProdiDetail.rumpun.in_(rumpun))
    if jenjang:
        conditions.append(ProdiDetail.jenjang == jenjang)
    if akreditasi:
        conditions.append(ProdiDetail.akreditasi == akreditasi)
    if ptn_pts:
        conditions.append(ProdiDetail.ptn_pts == ptn_pts)
    if ptkin:
        conditions.append(ProdiDetail.ptkin_non_ptkin == ptkin)
    if dikti:
        conditions.append(ProdiDetail.dikti_diktis == dikti)
    if provinsi:
        conditions.append(ProdiDetail.provinsi.ilike(f"%{provinsi}%"))

    if conditions:
        query = query.where(and_(*conditions))

    if pt:
        query = query.join(PerguruanTinggi, ProdiDetail.pt_id == PerguruanTinggi.id).where(
            PerguruanTinggi.nama.ilike(f"%{pt}%")
        )

    query = query.order_by(ProdiDetail.rumpun, ProdiDetail.nama_prodi)
    result = await db.execute(query)
    items = result.scalars().all()

    pt_ids = list(set(p.pt_id for p in items if p.pt_id))
    pt_map = {}
    if pt_ids:
        pt_r = await db.execute(select(PerguruanTinggi).where(PerguruanTinggi.id.in_(pt_ids)))
        pt_map = {p.id: p.nama for p in pt_r.scalars().all()}

    buffer = export_prodi_detail_excel(items, pt_map)

    filename = f"Data_Prodi_Ekonomi_Syariah_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    client_ip = request.client.host if request.client else None
    await log_activity(db, user.id, "export_prodi", f"Export {len(items)} prodi", client_ip)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ── Purge ──
@router.delete("/purge")
async def purge_prodi_detail(
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    running = await db.execute(select(ScrapeJob).where(ScrapeJob.status == "running"))
    if running.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Masih ada proses scraping yang berjalan.")

    total = (await db.execute(select(func.count()).select_from(ProdiDetail))).scalar() or 0
    await db.execute(delete(ProdiDetail))
    await db.commit()

    client_ip = request.client.host if request.client else None
    await log_activity(db, user.id, "purge_prodi_detail", f"Menghapus {total} prodi detail", client_ip)

    return {"message": f"{total} data prodi detail berhasil dihapus", "deleted": total}
