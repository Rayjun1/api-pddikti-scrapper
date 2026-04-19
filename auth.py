"""
PDDikti Dosen Explorer — Authentication & Authorization
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Query, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select, update, and_
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from database import get_db
from models import ScrapeJob, User, UserSession, UserActivity

settings = get_settings()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    except JWTError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Get current user from JWT token."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token diperlukan untuk akses"
        )

    token = credentials.credentials
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid atau sudah expired"
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid"
        )

    # Check active session
    result = await db.execute(
        select(UserSession).where(
            and_(
                UserSession.user_id == int(user_id),
                UserSession.token == token,
                UserSession.is_active == True
            )
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesi telah berakhir. Silakan login kembali."
        )

    # Check inactivity timeout
    if session.last_activity:
        inactivity = (datetime.now(timezone.utc) - session.last_activity.replace(tzinfo=timezone.utc)).total_seconds()
        if inactivity > settings.INACTIVITY_TIMEOUT:
            # Auto-logout
            await _invalidate_session(db, session, int(user_id))
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Sesi expired karena tidak ada aktivitas. Silakan login kembali."
            )

    # Update last activity
    session.last_activity = datetime.now(timezone.utc)
    await db.commit()

    # Get user
    result = await db.execute(select(User).where(User.id == int(user_id)))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Akun tidak ditemukan atau nonaktif"
        )

    # Update user last activity
    user.last_activity = datetime.now(timezone.utc)
    await db.commit()

    return user


async def get_current_user_for_download(
    request: Request,
    token_param: Optional[str] = Query(None, alias="token"),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Like get_current_user but also accepts token via query param (for browser downloads)."""
    # Prefer Authorization header, fall back to ?token= query param
    raw_token = credentials.credentials if credentials else token_param
    if not raw_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token diperlukan untuk akses"
        )

    payload = decode_token(raw_token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid atau sudah expired"
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid"
        )

    result = await db.execute(
        select(UserSession).where(
            and_(
                UserSession.user_id == int(user_id),
                UserSession.token == raw_token,
                UserSession.is_active == True
            )
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesi telah berakhir. Silakan login kembali."
        )

    if session.last_activity:
        inactivity = (datetime.now(timezone.utc) - session.last_activity.replace(tzinfo=timezone.utc)).total_seconds()
        if inactivity > settings.INACTIVITY_TIMEOUT:
            await _invalidate_session(db, session, int(user_id))
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Sesi expired karena tidak ada aktivitas. Silakan login kembali."
            )

    session.last_activity = datetime.now(timezone.utc)
    await db.commit()

    result = await db.execute(select(User).where(User.id == int(user_id)))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Akun tidak ditemukan atau nonaktif"
        )

    user.last_activity = datetime.now(timezone.utc)
    await db.commit()

    return user


async def _invalidate_session(db: AsyncSession, session: UserSession, user_id: int):
    """Invalidate a session and mark user offline."""
    session.is_active = False
    await db.execute(
        update(User).where(User.id == user_id).values(
            is_online=False,
            last_logout=datetime.now(timezone.utc)
        )
    )
    await db.commit()


async def _get_running_scrape_owner(
    db: AsyncSession,
    exclude_user_id: int = None,
) -> Optional[User]:
    """Treat a running scrape job as an active lock owner even after logout."""
    result = await db.execute(
        select(ScrapeJob)
        .where(and_(ScrapeJob.status == "running", ScrapeJob.user_id.isnot(None)))
        .order_by(ScrapeJob.started_at.desc())
    )
    running_jobs = result.scalars().all()

    for job in running_jobs:
        if exclude_user_id and job.user_id == exclude_user_id:
            continue

        user_result = await db.execute(select(User).where(User.id == job.user_id))
        lock_user = user_result.scalar_one_or_none()
        if lock_user and lock_user.role != "admin":
            return lock_user

    return None


async def get_admin_user(
    user: User = Depends(get_current_user),
) -> User:
    """Require admin role."""
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Akses khusus admin"
        )
    return user


async def check_single_user_lock(db: AsyncSession, exclude_user_id: int = None) -> Optional[User]:
    """
    Check if any user (non-admin) currently has an active session.
    Returns the active user if locked, None if free.
    """
    running_lock_owner = await _get_running_scrape_owner(db, exclude_user_id)
    if running_lock_owner:
        return running_lock_owner

    query = select(UserSession).where(UserSession.is_active == True)

    result = await db.execute(query)
    active_sessions = result.scalars().all()

    for session in active_sessions:
        # Check if it's timed out
        if session.last_activity:
            inactivity = (datetime.now(timezone.utc) - session.last_activity.replace(tzinfo=timezone.utc)).total_seconds()
            if inactivity > settings.INACTIVITY_TIMEOUT:
                # Auto-expire this session
                await _invalidate_session(db, session, session.user_id)
                continue

        # Skip excluded user and admin checking
        if exclude_user_id and session.user_id == exclude_user_id:
            continue

        # Get the user for this session
        user_result = await db.execute(select(User).where(User.id == session.user_id))
        active_user = user_result.scalar_one_or_none()
        if active_user and active_user.role != "admin":
            return active_user  # A non-admin is online

    return None


async def log_activity(
    db: AsyncSession, user_id: int, action: str,
    detail: str = None, ip_address: str = None
):
    """Log a user activity."""
    activity = UserActivity(
        user_id=user_id,
        action=action,
        detail=detail,
        ip_address=ip_address
    )
    db.add(activity)
    await db.commit()


async def create_default_admin(db: AsyncSession):
    """Create default admin user if not exists."""
    result = await db.execute(
        select(User).where(User.username == settings.ADMIN_USERNAME)
    )
    admin = result.scalar_one_or_none()

    if not admin:
        admin = User(
            username=settings.ADMIN_USERNAME,
            password_hash=hash_password(settings.ADMIN_PASSWORD),
            display_name="Administrator",
            role="admin",
            is_active=True,
        )
        db.add(admin)
        await db.commit()
        print(f"[OK] Default admin created: {settings.ADMIN_USERNAME}")
    else:
        print(f"[INFO] Admin '{settings.ADMIN_USERNAME}' already exists")
