"""
PDDikti Dosen Explorer — SQLAlchemy Models
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, Enum, JSON, Boolean,
    TIMESTAMP, ForeignKey, Index, func
)
from sqlalchemy.orm import relationship
from database import Base


class User(Base):
    """User accounts for the application."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    display_name = Column(String(255), nullable=False)
    role = Column(Enum("admin", "user"), default="user", nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_online = Column(Boolean, default=False, nullable=False)
    last_activity = Column(TIMESTAMP, nullable=True)
    last_login = Column(TIMESTAMP, nullable=True)
    last_logout = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    activities = relationship("UserActivity", back_populates="user", cascade="all, delete-orphan")
    sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_username", "username"),
        Index("idx_role", "role"),
    )


class UserSession(Base):
    """Active login sessions — only one user session allowed at a time."""
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    token = Column(String(500), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    last_activity = Column(TIMESTAMP, server_default=func.now())
    created_at = Column(TIMESTAMP, server_default=func.now())
    expires_at = Column(TIMESTAMP, nullable=True)

    user = relationship("User", back_populates="sessions")

    __table_args__ = (
        Index("idx_session_active", "is_active"),
        Index("idx_session_user", "user_id"),
    )


class UserActivity(Base):
    """Track user actions for admin monitoring."""
    __tablename__ = "user_activities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    action = Column(String(100), nullable=False)  # login, logout, scrape_start, export, etc
    detail = Column(Text, nullable=True)
    ip_address = Column(String(45), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())

    user = relationship("User", back_populates="activities")

    __table_args__ = (
        Index("idx_activity_user", "user_id"),
        Index("idx_activity_created", "created_at"),
    )


class PerguruanTinggi(Base):
    __tablename__ = "perguruan_tinggi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pddikti_id = Column(String(255), unique=True, nullable=True)
    kode_pt = Column(String(20), nullable=True)
    nama = Column(String(255), nullable=False)
    nama_singkat = Column(String(50), nullable=True)
    alamat = Column(Text, nullable=True)
    provinsi = Column(String(100), nullable=True)
    kab_kota = Column(String(100), nullable=True)
    website = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    status = Column(String(50), nullable=True)
    akreditasi = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    kelompok = Column(String(50), nullable=True)       # e.g. PTKIN / NON PTKIN
    pembina = Column(String(50), nullable=True)        # e.g. DIKTI / DIKTIS
    status_pt = Column(String(50), nullable=True)      # e.g. PTN / PTS

    program_studi = relationship("ProgramStudi", back_populates="pt")
    prodi_details = relationship("ProdiDetail", back_populates="pt")
    dosen = relationship("Dosen", back_populates="pt")

    __table_args__ = (
        Index("idx_pt_nama", "nama"),
        Index("idx_pt_status", "status"),
    )


class ProgramStudi(Base):
    __tablename__ = "program_studi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pddikti_id = Column(String(255), unique=True, nullable=True)
    pt_id = Column(Integer, ForeignKey("perguruan_tinggi.id"), nullable=True)
    nama = Column(String(255), nullable=False)
    rumpun = Column(String(100), nullable=True)
    jenjang = Column(String(10), nullable=True)
    kode_prodi = Column(String(20), nullable=True)
    bidang_ilmu = Column(String(100), nullable=True)
    status = Column(String(50), nullable=True)
    akreditasi = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    pt = relationship("PerguruanTinggi", back_populates="program_studi")
    dosen = relationship("Dosen", back_populates="prodi")

    __table_args__ = (
        Index("idx_prodi_nama", "nama"),
        Index("idx_prodi_rumpun", "rumpun"),
        Index("idx_prodi_jenjang", "jenjang"),
    )


class Dosen(Base):
    __tablename__ = "dosen"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pddikti_id = Column(String(255), unique=True, nullable=True)
    nidn = Column(String(20), unique=True, nullable=True)
    nuptk = Column(String(30), nullable=True)
    nama = Column(String(255), nullable=False)
    jenis_kelamin = Column(String(20), nullable=True)
    jabatan_fungsional = Column(String(100), nullable=True)
    pendidikan_terakhir = Column(String(20), nullable=True)
    status_ikatan_kerja = Column(String(100), nullable=True)
    status_aktivitas = Column(String(50), nullable=True)
    status_kepegawaian = Column(String(50), nullable=True)   # PNS / NON ASN (dari Excel)
    jenjang_prodi = Column(String(10), nullable=True)        # S1/S2/D4 prodi tempat dosen mengajar
    semester_data = Column(String(10), nullable=True)        # Semester data terakhir dosen
    pt_id = Column(Integer, ForeignKey("perguruan_tinggi.id"), nullable=True)
    prodi_id = Column(Integer, ForeignKey("program_studi.id"), nullable=True)
    rumpun_prodi = Column(String(100), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    pt = relationship("PerguruanTinggi", back_populates="dosen")
    prodi = relationship("ProgramStudi", back_populates="dosen")

    __table_args__ = (
        Index("idx_dosen_nama", "nama"),
        Index("idx_dosen_nidn", "nidn"),
        Index("idx_dosen_rumpun", "rumpun_prodi"),
        Index("idx_dosen_jabatan", "jabatan_fungsional"),
        Index("idx_dosen_jk", "jenis_kelamin"),
        Index("idx_dosen_pendidikan", "pendidikan_terakhir"),
        Index("idx_dosen_status", "status_aktivitas"),
        Index("idx_dosen_kepegawaian", "status_kepegawaian"),
    )



class ProdiDetail(Base):
    """Scraped program studi detail — one row per prodi across all campuses."""
    __tablename__ = "prodi_detail"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pddikti_id = Column(String(255), unique=True, nullable=True)
    pt_id = Column(Integer, ForeignKey("perguruan_tinggi.id"), nullable=True)
    nama_prodi = Column(String(255), nullable=False)
    rumpun = Column(String(100), nullable=True)
    jenjang = Column(String(10), nullable=True)
    kode_prodi = Column(String(20), nullable=True)
    jumlah_dosen = Column(Integer, default=0)
    keterangan = Column(String(100), nullable=True)       # Aktif / Tidak Aktif
    akreditasi = Column(String(100), nullable=True)       # Baik, Baik Sekali, Unggul, …
    status_akreditasi = Column(String(100), nullable=True) # Belum Terakreditasi, dst
    ptn_pts = Column(String(10), nullable=True)           # PTN / PTS
    ptkin_non_ptkin = Column(String(20), nullable=True)   # PTKIN / NON PTKIN
    dikti_diktis = Column(String(20), nullable=True)      # DIKTI / DIKTIS
    provinsi = Column(String(100), nullable=True)
    semester_terakhir = Column(String(10), nullable=True)  # e.g. 20251
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    pt = relationship("PerguruanTinggi", back_populates="prodi_details")

    __table_args__ = (
        Index("idx_pd_nama", "nama_prodi"),
        Index("idx_pd_rumpun", "rumpun"),
        Index("idx_pd_jenjang", "jenjang"),
        Index("idx_pd_akreditasi", "akreditasi"),
        Index("idx_pd_ptn_pts", "ptn_pts"),
        Index("idx_pd_ptkin", "ptkin_non_ptkin"),
        Index("idx_pd_provinsi", "provinsi"),
    )


class ScrapeJob(Base):
    __tablename__ = "scrape_job"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    status = Column(
        Enum("pending", "running", "completed", "failed", "cancelled"),
        default="pending"
    )
    prodi_filter = Column(JSON, nullable=True)
    total_prodi = Column(Integer, default=0)
    resolved_prodi = Column(Integer, default=0)
    total_dosen = Column(Integer, default=0)
    new_dosen = Column(Integer, default=0)
    skipped_dosen = Column(Integer, default=0)
    total_prodi_detail = Column(Integer, default=0)
    new_prodi_detail = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())

    logs = relationship("ScrapeLog", back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_job_status", "status"),
        Index("idx_job_created", "created_at"),
    )


class ScrapeLog(Base):
    __tablename__ = "scrape_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("scrape_job.id", ondelete="CASCADE"), nullable=False)
    level = Column(Enum("info", "warning", "error", "success"), default="info")
    message = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())

    job = relationship("ScrapeJob", back_populates="logs")

    __table_args__ = (
        Index("idx_log_job", "job_id"),
    )
