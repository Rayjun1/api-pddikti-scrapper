"""
PDDikti Dosen Explorer — Stats Router
Dashboard statistics.
"""

from fastapi import APIRouter, Depends
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user
from database import get_db
from models import Dosen, PerguruanTinggi, ProgramStudi, ScrapeJob, User

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("")
async def get_stats(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get dashboard statistics."""
    # Totals
    total_dosen = (await db.execute(select(func.count()).select_from(Dosen))).scalar() or 0
    total_program_studi = (await db.execute(select(func.count()).select_from(ProgramStudi))).scalar() or 0
    total_pt = (await db.execute(select(func.count()).select_from(PerguruanTinggi))).scalar() or 0

    # By gender
    gender_q = await db.execute(
        select(Dosen.jenis_kelamin, func.count(Dosen.id))
        .where(Dosen.jenis_kelamin.isnot(None))
        .where(Dosen.jenis_kelamin != "")
        .group_by(Dosen.jenis_kelamin)
    )
    by_gender = {r[0]: r[1] for r in gender_q.all()}

    # By jabatan
    jabatan_q = await db.execute(
        select(Dosen.jabatan_fungsional, func.count(Dosen.id))
        .where(Dosen.jabatan_fungsional.isnot(None))
        .where(Dosen.jabatan_fungsional != "")
        .group_by(Dosen.jabatan_fungsional)
        .order_by(desc(func.count(Dosen.id)))
    )
    by_jabatan = {r[0]: r[1] for r in jabatan_q.all()}

    # By pendidikan
    pendidikan_q = await db.execute(
        select(Dosen.pendidikan_terakhir, func.count(Dosen.id))
        .where(Dosen.pendidikan_terakhir.isnot(None))
        .where(Dosen.pendidikan_terakhir != "")
        .group_by(Dosen.pendidikan_terakhir)
        .order_by(desc(func.count(Dosen.id)))
    )
    by_pendidikan = {r[0]: r[1] for r in pendidikan_q.all()}

    # By rumpun
    rumpun_q = await db.execute(
        select(Dosen.rumpun_prodi, func.count(Dosen.id))
        .where(Dosen.rumpun_prodi.isnot(None))
        .group_by(Dosen.rumpun_prodi)
        .order_by(desc(func.count(Dosen.id)))
    )
    by_rumpun = {r[0]: r[1] for r in rumpun_q.all()}
    total_rumpun = len(by_rumpun)

    # By status
    status_q = await db.execute(
        select(Dosen.status_aktivitas, func.count(Dosen.id))
        .where(Dosen.status_aktivitas.isnot(None))
        .where(Dosen.status_aktivitas != "")
        .group_by(Dosen.status_aktivitas)
    )
    by_status = {r[0]: r[1] for r in status_q.all()}

    # Last scrape
    last_job = await db.execute(
        select(ScrapeJob)
        .where(ScrapeJob.status == "completed")
        .order_by(desc(ScrapeJob.completed_at))
        .limit(1)
    )
    last_scrape_job = last_job.scalar_one_or_none()

    return {
        "total_dosen": total_dosen,
        "total_prodi": total_program_studi,
        "total_program_studi": total_program_studi,
        "total_rumpun": total_rumpun,
        "total_pt": total_pt,
        "by_gender": by_gender,
        "by_jabatan": by_jabatan,
        "by_pendidikan": by_pendidikan,
        "by_rumpun": by_rumpun,
        "by_status": by_status,
        "last_scrape": last_scrape_job.completed_at.isoformat() if last_scrape_job and last_scrape_job.completed_at else None,
    }


@router.get("/rumpun")
async def get_rumpun_list(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get the 12 official rumpun categories with counts."""
    from services.scraper import RUMPUN_PRODI_RESMI

    rumpun_q = await db.execute(
        select(Dosen.rumpun_prodi, func.count(Dosen.id))
        .where(Dosen.rumpun_prodi.isnot(None))
        .group_by(Dosen.rumpun_prodi)
    )
    counts = {r[0]: r[1] for r in rumpun_q.all()}

    return {
        "rumpun": [
            {"nama": r, "count": counts.get(r, 0)}
            for r in RUMPUN_PRODI_RESMI
        ]
    }
