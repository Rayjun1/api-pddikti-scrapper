"""
PDDikti Dosen Explorer — Database Connection
"""

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from config import get_settings

settings = get_settings()

# SQLite tidak support connection pooling parameters
_is_sqlite = settings.DATABASE_URL.startswith("sqlite")

if _is_sqlite:
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )
else:
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
    )

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


def _get_column_sql(col):
    """Get column type string from a SQLAlchemy Column (MySQL & SQLite compatible)."""
    from sqlalchemy import Integer, String, Text, Boolean, TIMESTAMP, JSON, Enum as SAEnum
    col_type = col.type
    if isinstance(col_type, Integer):
        return "INTEGER"
    elif isinstance(col_type, String):
        return f"VARCHAR({col_type.length})" if col_type.length else "VARCHAR(255)"
    elif isinstance(col_type, Text):
        return "TEXT"
    elif isinstance(col_type, Boolean):
        return "INTEGER"  # SQLite uses INTEGER for booleans
    elif isinstance(col_type, TIMESTAMP):
        return "TIMESTAMP NULL"
    elif isinstance(col_type, JSON):
        return "TEXT"  # SQLite stores JSON as TEXT
    elif isinstance(col_type, SAEnum):
        return "TEXT"  # SQLite doesn't have ENUM
    return "TEXT"


def _sync_columns(conn):
    """Add missing columns to existing tables (simple auto-migration).
    Catatan: fungsi ini dipanggil via run_sync() sehingga HARUS sync (bukan async).
    """
    inspector = inspect(conn)
    existing_tables = inspector.get_table_names()
    is_sqlite = settings.DATABASE_URL.startswith("sqlite")

    for table_name, table in Base.metadata.tables.items():
        if table_name not in existing_tables:
            continue  # Table will be created by create_all

        existing_cols = {c["name"] for c in inspector.get_columns(table_name)}
        for col in table.columns:
            if col.name not in existing_cols:
                col_sql = _get_column_sql(col)
                default = ""
                if col.default is not None and hasattr(col.default, "arg"):
                    if isinstance(col.default.arg, int):
                        default = f" DEFAULT {col.default.arg}"
                    elif isinstance(col.default.arg, str):
                        default = f" DEFAULT '{col.default.arg}'"

                # SQLite uses plain quotes; MySQL uses backticks
                if is_sqlite:
                    stmt = f'ALTER TABLE "{table_name}" ADD COLUMN "{col.name}" {col_sql}{default}'
                else:
                    stmt = f"ALTER TABLE `{table_name}` ADD COLUMN `{col.name}` {col_sql}{default}"
                print(f"  ➕ Adding column: {table_name}.{col.name}")
                conn.execute(text(stmt))


async def init_db():
    """Create all tables on startup and sync missing columns."""
    async with engine.begin() as conn:
        # First, sync columns on existing tables
        await conn.run_sync(_sync_columns)
        # Then create any new tables
        await conn.run_sync(Base.metadata.create_all)
