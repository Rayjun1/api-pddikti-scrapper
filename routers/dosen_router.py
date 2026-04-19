"""
PDDikti Dosen Explorer — Dosen Router
List, detail, and export dosen data.
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
from models import Dosen, PerguruanTinggi, ProdiDetail, ProgramStudi, ScrapeJob, User

router = APIRouter(prefix="/api/dosen", tags=["dosen"])


@router.get("")
async def list_dosen(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    prodi: Optional[List[str]] = Query(None),
    rumpun: Optional[List[str]] = Query(None),
    jabatan: Optional[str] = Query(None),
    jenis_kelamin: Optional[str] = Query(None),
    pendidikan: Optional[str] = Query(None),
    status_aktivitas: Optional[str] = Query(None, alias="status"),
    status_ikatan_kerja: Optional[str] = Query(None, alias="ikatan_kerja"),
    pt: Optional[str] = Query(None),
    sort_by: str = Query("nama"),
    sort_order: str = Query("asc"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List dosen with pagination, filtering, and sorting."""
    query = select(Dosen)

    # Filters
    conditions = []

    if search:
        search_term = f"%{search}%"
        conditions.append(
            or_(
                Dosen.nama.ilike(search_term),
                Dosen.nidn.ilike(search_term),
            )
        )

    if rumpun:
        conditions.append(Dosen.rumpun_prodi.in_(rumpun))
    if jabatan:
        conditions.append(Dosen.jabatan_fungsional == jabatan)
    if jenis_kelamin:
        conditions.append(Dosen.jenis_kelamin == jenis_kelamin)
    if pendidikan:
        conditions.append(Dosen.pendidikan_terakhir == pendidikan)
    if status_aktivitas:
        conditions.append(Dosen.status_aktivitas == status_aktivitas)
    if status_ikatan_kerja:
        conditions.append(Dosen.status_ikatan_kerja == status_ikatan_kerja)

    if conditions:
        query = query.where(and_(*conditions))

    # If filtering by PT name, join
    if pt:
        query = query.join(PerguruanTinggi, Dosen.pt_id == PerguruanTinggi.id).where(
            PerguruanTinggi.nama.ilike(f"%{pt}%")
        )

    # If filtering by prodi name
    if prodi:
        query = query.join(ProgramStudi, Dosen.prodi_id == ProgramStudi.id).where(
            ProgramStudi.nama.in_(prodi)
        )

    # Count total
    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar()

    # Sort
    sort_col = {
        "nama": Dosen.nama,
        "nidn": Dosen.nidn,
        "jabatan": Dosen.jabatan_fungsional,
        "pendidikan": Dosen.pendidikan_terakhir,
        "rumpun": Dosen.rumpun_prodi,
    }.get(sort_by, Dosen.nama)

    order = desc(sort_col) if sort_order == "desc" else asc(sort_col)
    query = query.order_by(order)

    # Paginate
    query = query.offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(query)
    dosen_list = result.scalars().all()

    # Get PT and Prodi names
    pt_ids = list(set(d.pt_id for d in dosen_list if d.pt_id))
    prodi_ids = list(set(d.prodi_id for d in dosen_list if d.prodi_id))

    pt_map = {}
    if pt_ids:
        pt_result = await db.execute(select(PerguruanTinggi).where(PerguruanTinggi.id.in_(pt_ids)))
        pt_map = {p.id: p.nama for p in pt_result.scalars().all()}

    prodi_map = {}
    if prodi_ids:
        prodi_result = await db.execute(select(ProgramStudi).where(ProgramStudi.id.in_(prodi_ids)))
        prodi_map = {p.id: p.nama for p in prodi_result.scalars().all()}

    return {
        "data": [
            {
                "id": d.id,
                "nidn": d.nidn or "",
                "nuptk": d.nuptk or "",
                "nama": d.nama,
                "jenis_kelamin": d.jenis_kelamin or "",
                "jabatan_fungsional": d.jabatan_fungsional or "",
                "pendidikan_terakhir": d.pendidikan_terakhir or "",
                "status_ikatan_kerja": d.status_ikatan_kerja or "",
                "status_aktivitas": d.status_aktivitas or "",
                "perguruan_tinggi": pt_map.get(d.pt_id, ""),
                "program_studi": prodi_map.get(d.prodi_id, ""),
                "rumpun_prodi": d.rumpun_prodi or "",
            }
            for d in dosen_list
        ],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if total else 0,
        },
    }


@router.delete("/purge")
async def purge_scraped_data(
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete all stored dosen scraping results while keeping job history intact."""
    running_job = await db.execute(
        select(ScrapeJob).where(ScrapeJob.status == "running")
    )
    if running_job.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail="Masih ada proses scraping yang berjalan. Stop dulu sebelum menghapus hasil scraping."
        )

    total_dosen = (await db.execute(select(func.count()).select_from(Dosen))).scalar() or 0
    total_prodi = (await db.execute(select(func.count()).select_from(ProgramStudi))).scalar() or 0
    total_prodi_detail = (await db.execute(select(func.count()).select_from(ProdiDetail))).scalar() or 0
    total_pt = (await db.execute(select(func.count()).select_from(PerguruanTinggi))).scalar() or 0

    await db.execute(delete(Dosen))
    await db.execute(delete(ProdiDetail))
    await db.execute(delete(ProgramStudi))
    await db.execute(delete(PerguruanTinggi))
    await db.commit()

    client_ip = request.client.host if request.client else None
    await log_activity(
        db,
        user.id,
        "purge_scrape_data",
        f"Menghapus hasil scraping: {total_dosen} dosen, {total_prodi_detail} prodi detail, {total_prodi} prodi, {total_pt} PT",
        client_ip,
    )

    return {
        "message": "Semua hasil scraping di database berhasil dihapus",
        "deleted": {
            "dosen": total_dosen,
            "prodi_detail": total_prodi_detail,
            "program_studi": total_prodi,
            "perguruan_tinggi": total_pt,
        },
    }


@router.get("/filters")
async def get_filter_options(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get available filter options for dropdowns."""
    # Rumpun
    rumpun_q = await db.execute(
        select(Dosen.rumpun_prodi, func.count(Dosen.id))
        .where(Dosen.rumpun_prodi.isnot(None))
        .group_by(Dosen.rumpun_prodi)
        .order_by(desc(func.count(Dosen.id)))
    )
    rumpun_options = [{"value": r[0], "count": r[1]} for r in rumpun_q.all()]

    # Jabatan
    jabatan_q = await db.execute(
        select(Dosen.jabatan_fungsional, func.count(Dosen.id))
        .where(Dosen.jabatan_fungsional.isnot(None))
        .where(Dosen.jabatan_fungsional != "")
        .group_by(Dosen.jabatan_fungsional)
        .order_by(desc(func.count(Dosen.id)))
    )
    jabatan_options = [{"value": r[0], "count": r[1]} for r in jabatan_q.all()]

    # Pendidikan
    pendidikan_q = await db.execute(
        select(Dosen.pendidikan_terakhir, func.count(Dosen.id))
        .where(Dosen.pendidikan_terakhir.isnot(None))
        .where(Dosen.pendidikan_terakhir != "")
        .group_by(Dosen.pendidikan_terakhir)
        .order_by(desc(func.count(Dosen.id)))
    )
    pendidikan_options = [{"value": r[0], "count": r[1]} for r in pendidikan_q.all()]

    # Gender
    gender_q = await db.execute(
        select(Dosen.jenis_kelamin, func.count(Dosen.id))
        .where(Dosen.jenis_kelamin.isnot(None))
        .where(Dosen.jenis_kelamin != "")
        .group_by(Dosen.jenis_kelamin)
        .order_by(desc(func.count(Dosen.id)))
    )
    gender_options = [{"value": r[0], "count": r[1]} for r in gender_q.all()]

    # PT names
    pt_q = await db.execute(
        select(PerguruanTinggi.nama, func.count(Dosen.id))
        .join(Dosen, Dosen.pt_id == PerguruanTinggi.id)
        .group_by(PerguruanTinggi.nama)
        .order_by(PerguruanTinggi.nama)
    )
    pt_options = [{"value": r[0], "count": r[1]} for r in pt_q.all()]

    # Status ikatan kerja
    ikatan_q = await db.execute(
        select(Dosen.status_ikatan_kerja, func.count(Dosen.id))
        .where(Dosen.status_ikatan_kerja.isnot(None))
        .where(Dosen.status_ikatan_kerja != "")
        .group_by(Dosen.status_ikatan_kerja)
        .order_by(desc(func.count(Dosen.id)))
    )
    ikatan_options = [{"value": r[0], "count": r[1]} for r in ikatan_q.all()]

    # Status aktivitas
    status_q = await db.execute(
        select(Dosen.status_aktivitas, func.count(Dosen.id))
        .where(Dosen.status_aktivitas.isnot(None))
        .where(Dosen.status_aktivitas != "")
        .group_by(Dosen.status_aktivitas)
        .order_by(desc(func.count(Dosen.id)))
    )
    status_options = [{"value": r[0], "count": r[1]} for r in status_q.all()]

    return {
        "rumpun": rumpun_options,
        "jabatan": jabatan_options,
        "pendidikan": pendidikan_options,
        "jenis_kelamin": gender_options,
        "perguruan_tinggi": pt_options,
        "status_ikatan_kerja": ikatan_options,
        "status_aktivitas": status_options,
    }


@router.get("/export")
async def export_excel(
    request: Request,
    search: Optional[str] = Query(None),
    rumpun: Optional[List[str]] = Query(None),
    jabatan: Optional[str] = Query(None),
    jenis_kelamin: Optional[str] = Query(None),
    pendidikan: Optional[str] = Query(None),
    status_aktivitas: Optional[str] = Query(None, alias="status"),
    status_ikatan_kerja: Optional[str] = Query(None, alias="ikatan_kerja"),
    pt: Optional[str] = Query(None),
    user: User = Depends(get_current_user_for_download),
    db: AsyncSession = Depends(get_db),
):
    """Export filtered dosen data to Excel."""
    from services.exporter import export_dosen_excel

    query = select(Dosen)
    conditions = []

    if search:
        conditions.append(or_(Dosen.nama.ilike(f"%{search}%"), Dosen.nidn.ilike(f"%{search}%")))
    if rumpun:
        conditions.append(Dosen.rumpun_prodi.in_(rumpun))
    if jabatan:
        conditions.append(Dosen.jabatan_fungsional == jabatan)
    if jenis_kelamin:
        conditions.append(Dosen.jenis_kelamin == jenis_kelamin)
    if pendidikan:
        conditions.append(Dosen.pendidikan_terakhir == pendidikan)
    if status_aktivitas:
        conditions.append(Dosen.status_aktivitas == status_aktivitas)
    if status_ikatan_kerja:
        conditions.append(Dosen.status_ikatan_kerja == status_ikatan_kerja)

    if conditions:
        query = query.where(and_(*conditions))

    if pt:
        query = query.join(PerguruanTinggi, Dosen.pt_id == PerguruanTinggi.id).where(
            PerguruanTinggi.nama.ilike(f"%{pt}%")
        )

    query = query.order_by(Dosen.rumpun_prodi, Dosen.nama)
    result = await db.execute(query)
    dosen_list = result.scalars().all()

    # Resolve PT names
    pt_ids = list(set(d.pt_id for d in dosen_list if d.pt_id))
    pt_map = {}
    if pt_ids:
        pt_result = await db.execute(select(PerguruanTinggi).where(PerguruanTinggi.id.in_(pt_ids)))
        pt_map = {p.id: p.nama for p in pt_result.scalars().all()}

    buffer = export_dosen_excel(dosen_list, pt_map)

    client_ip = request.client.host if request.client else None
    await log_activity(db, user.id, "export_excel", f"Exported {len(dosen_list)} dosen", client_ip)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data_dosen_ekosyariah_{timestamp}.xlsx"

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.document",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/{dosen_id}")
async def get_dosen_detail(
    dosen_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get single dosen detail."""
    result = await db.execute(select(Dosen).where(Dosen.id == dosen_id))
    d = result.scalar_one_or_none()
    if not d:
        raise HTTPException(status_code=404, detail="Dosen tidak ditemukan")

    pt_name = ""
    if d.pt_id:
        pt_result = await db.execute(select(PerguruanTinggi).where(PerguruanTinggi.id == d.pt_id))
        pt_obj = pt_result.scalar_one_or_none()
        if pt_obj:
            pt_name = pt_obj.nama

    prodi_name = ""
    if d.prodi_id:
        prodi_result = await db.execute(select(ProgramStudi).where(ProgramStudi.id == d.prodi_id))
        prodi_obj = prodi_result.scalar_one_or_none()
        if prodi_obj:
            prodi_name = prodi_obj.nama

    return {
        "id": d.id,
        "nidn": d.nidn or "",
        "nuptk": d.nuptk or "",
        "nama": d.nama,
        "jenis_kelamin": d.jenis_kelamin or "",
        "jabatan_fungsional": d.jabatan_fungsional or "",
        "pendidikan_terakhir": d.pendidikan_terakhir or "",
        "status_ikatan_kerja": d.status_ikatan_kerja or "",
        "status_aktivitas": d.status_aktivitas or "",
        "perguruan_tinggi": pt_name,
        "program_studi": prodi_name,
        "rumpun_prodi": d.rumpun_prodi or "",
    }
