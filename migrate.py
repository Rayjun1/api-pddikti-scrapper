"""
Quick migration script — run on VPS to add missing columns.
Usage: python3 migrate.py
"""

import pymysql
from config import get_settings

settings = get_settings()

MIGRATIONS = [
    # scrape_job — missing columns
    "ALTER TABLE `scrape_job` ADD COLUMN `total_prodi_detail` INTEGER DEFAULT 0",
    "ALTER TABLE `scrape_job` ADD COLUMN `new_prodi_detail` INTEGER DEFAULT 0",
    # perguruan_tinggi — missing columns
    "ALTER TABLE `perguruan_tinggi` ADD COLUMN `kelompok` VARCHAR(50) NULL",
    "ALTER TABLE `perguruan_tinggi` ADD COLUMN `pembina` VARCHAR(50) NULL",
    "ALTER TABLE `perguruan_tinggi` ADD COLUMN `status_pt` VARCHAR(50) NULL",
]


def run():
    conn = pymysql.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
    )
    cursor = conn.cursor()

    for sql in MIGRATIONS:
        try:
            cursor.execute(sql)
            print(f"✅ {sql}")
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1060:  # Duplicate column name — already exists
                col = sql.split("ADD COLUMN")[1].strip().split()[0].strip("`")
                print(f"⏭️  Column already exists, skipping: {col}")
            else:
                print(f"❌ Error: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("\n🎉 Migration complete!")


if __name__ == "__main__":
    run()
