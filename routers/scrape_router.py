"""
PDDikti Dosen Explorer — Scrape Router
Start, monitor, and manage scraping jobs.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from sqlalchemy import select, func, desc, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, log_activity
from database import get_db, AsyncSessionLocal
from models import ScrapeJob, ScrapeLog, User

router = APIRouter(prefix="/api/scrape", tags=["scrape"])

# Active WebSocket connections per job
active_connections: dict[int, list[WebSocket]] = {}
# Running tasks
running_tasks: dict[int, asyncio.Task] = {}


class ScrapeStartRequest(BaseModel):
    prodi_filter: List[str]
    semesters: Optional[List[str]] = None
    pt_filter: Optional[str] = None


@router.post("/start")
async def start_scrape(
    req: ScrapeStartRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Start a new scraping job."""
    # Check if there's already a running job
    running = await db.execute(
        select(ScrapeJob).where(ScrapeJob.status == "running")
    )
    if running.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail="Sudah ada scraping yang sedang berjalan. Tunggu selesai atau cancel dulu."
        )

    if not req.prodi_filter:
        raise HTTPException(status_code=400, detail="Pilih minimal 1 rumpun prodi")

    # Create job
    job = ScrapeJob(
        user_id=user.id,
        status="running",
        prodi_filter=req.prodi_filter,
        started_at=datetime.now(timezone.utc),
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Start scraping in background
    from services.scraper import run_scraping_job
    task = asyncio.create_task(run_scraping_job(job.id, req.prodi_filter, req.semesters, req.pt_filter))
    running_tasks[job.id] = task

    client_ip = request.client.host if request.client else None
    await log_activity(
        db, user.id, "scrape_start",
        f"Started job #{job.id}: {', '.join(req.prodi_filter)}",
        client_ip
    )

    return {
        "job_id": job.id,
        "status": "running",
        "message": f"Scraping dimulai untuk {len(req.prodi_filter)} rumpun prodi"
    }


@router.get("/jobs")
async def list_jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all scraping jobs."""
    count_q = select(func.count()).select_from(ScrapeJob)
    total = (await db.execute(count_q)).scalar()

    result = await db.execute(
        select(ScrapeJob)
        .order_by(desc(ScrapeJob.created_at))
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    jobs = result.scalars().all()

    return {
        "jobs": [
            {
                "id": j.id,
                "status": j.status,
                "prodi_filter": j.prodi_filter,
                "total_prodi": j.total_prodi,
                "resolved_prodi": j.resolved_prodi,
                "total_dosen": j.total_dosen,
                "new_dosen": j.new_dosen,
                "skipped_dosen": j.skipped_dosen,
                "error_message": j.error_message,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in jobs
        ],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if total else 0,
        }
    }


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get job detail with recent logs."""
    result = await db.execute(select(ScrapeJob).where(ScrapeJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")

    # Get recent logs
    logs_result = await db.execute(
        select(ScrapeLog)
        .where(ScrapeLog.job_id == job_id)
        .order_by(desc(ScrapeLog.created_at))
        .limit(100)
    )
    logs = logs_result.scalars().all()

    return {
        "id": job.id,
        "status": job.status,
        "prodi_filter": job.prodi_filter,
        "total_prodi": job.total_prodi,
        "resolved_prodi": job.resolved_prodi,
        "total_dosen": job.total_dosen,
        "new_dosen": job.new_dosen,
        "skipped_dosen": job.skipped_dosen,
        "error_message": job.error_message,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "logs": [
            {
                "id": l.id,
                "level": l.level,
                "message": l.message,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in reversed(logs)
        ]
    }


@router.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: int,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Cancel a running job."""
    result = await db.execute(select(ScrapeJob).where(ScrapeJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")

    if job.status != "running":
        raise HTTPException(status_code=400, detail="Job tidak sedang berjalan")

    # Cancel the task
    if job_id in running_tasks:
        running_tasks[job_id].cancel()
        del running_tasks[job_id]

    job.status = "cancelled"
    job.completed_at = datetime.now(timezone.utc)
    job.error_message = "Dibatalkan oleh user"

    # Add log
    cancel_log = ScrapeLog(
        job_id=job_id,
        level="warning",
        message="⚠️ Scraping dibatalkan oleh user"
    )
    db.add(cancel_log)
    await db.commit()

    # Notify WebSocket clients
    await broadcast_to_job(job_id, {
        "type": "cancelled",
        "message": "Scraping dibatalkan"
    })

    client_ip = request.client.host if request.client else None
    await log_activity(db, user.id, "scrape_cancel", f"Cancelled job #{job_id}", client_ip)

    return {"message": f"Job #{job_id} dibatalkan"}


@router.get("/active")
async def get_active_job(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Check if there's an active scraping job."""
    result = await db.execute(
        select(ScrapeJob).where(ScrapeJob.status == "running")
    )
    job = result.scalar_one_or_none()
    if job:
        return {
            "has_active": True,
            "job_id": job.id,
            "prodi_filter": job.prodi_filter,
            "resolved_prodi": job.resolved_prodi,
            "total_prodi": job.total_prodi,
            "total_dosen": job.total_dosen,
            "new_dosen": job.new_dosen,
        }
    return {"has_active": False}


# ── WebSocket ──

async def broadcast_to_job(job_id: int, data: dict):
    """Send message to all WebSocket clients listening to a job."""
    import json
    if job_id in active_connections:
        dead = []
        for ws in active_connections[job_id]:
            try:
                await ws.send_text(json.dumps(data))
            except Exception:
                dead.append(ws)
        for ws in dead:
            active_connections[job_id].remove(ws)


@router.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: int):
    """WebSocket endpoint for live scraping log streaming."""
    await websocket.accept()

    if job_id not in active_connections:
        active_connections[job_id] = []
    active_connections[job_id].append(websocket)

    try:
        while True:
            # Keep connection alive, wait for client messages (ping)
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        if job_id in active_connections:
            active_connections[job_id].remove(websocket)
            if not active_connections[job_id]:
                del active_connections[job_id]
    except Exception:
        if job_id in active_connections and websocket in active_connections[job_id]:
            active_connections[job_id].remove(websocket)
