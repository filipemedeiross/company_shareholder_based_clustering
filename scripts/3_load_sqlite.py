import time
import sqlite3

import pandas as pd
import pyarrow.parquet as pq

from pathlib import Path


BATCH_SIZE = 300000

ROOT_DIR     = Path(__file__).resolve().parent.parent
PARQUET_DIR  = ROOT_DIR / 'data/parquet'
SQLITE_PATH  = ROOT_DIR / 'data/sqlite/rfb.db'

PARQUET_PARTNERS  = PARQUET_DIR / 'partners.parquet'
PARQUET_COMPANIES = PARQUET_DIR / 'companies.parquet'
PARQUET_BUSINESS  = PARQUET_DIR / 'business.parquet'

SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)


def pad_zero(number, width):
    return str(number).zfill(width)

def int_to_date(value):
    try:
        return pd.to_datetime(str(value), format='%Y%m%d').date()
    except:
        return None

def measure_query_time(cursor, query, label):
    start = time.perf_counter()
    cursor.execute(query).fetchall()
    end   = time.perf_counter()

    print(f"⏱️ Query time for {label}: {end - start:.2f} seconds")


# ===================================
# 🔌 SQLite connection + table setup
# ===================================
conn   = sqlite3.connect(SQLITE_PATH)
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS partners')
cursor.execute('''
    CREATE TABLE partners (
        cnpj TEXT,
        name_partner TEXT,
        start_date DATE
    )
''')

cursor.execute('DROP TABLE IF EXISTS companies')
cursor.execute('''
    CREATE TABLE companies (
        cnpj TEXT,
        corporate_name TEXT,
        capital REAL
    )
''')

cursor.execute('DROP TABLE IF EXISTS business')
cursor.execute('''
    CREATE TABLE business (
        cnpj TEXT,
        cnpj_order TEXT,
        cnpj_dv TEXT,
        branch BOOLEAN,
        trade_name TEXT,
        closing_date DATE,
        opening_date DATE,
        cep TEXT
    )
''')

conn.commit()

# ===================================
# 📥 Load, transform and insert data
# ===================================
df = pd.read_parquet(PARQUET_PARTNERS)
df.cnpj       = df.cnpj.apply(lambda x: pad_zero(x, 8))
df.start_date = df.start_date.apply(int_to_date)
df.to_sql('partners', conn, index=False, if_exists='append')


df = pd.read_parquet(PARQUET_COMPANIES)
df.cnpj = df.cnpj.apply(lambda x: pad_zero(x, 8))
df.to_sql('companies', conn, index=False, if_exists='append')


table = pq.ParquetFile(PARQUET_BUSINESS)
for batch in table.iter_batches(batch_size=BATCH_SIZE):
    df = batch.to_pandas()

    df.cnpj       = df.cnpj.apply      (lambda x: pad_zero(x, 8))
    df.cnpj_order = df.cnpj_order.apply(lambda x: pad_zero(x, 4))
    df.cnpj_dv    = df.cnpj_dv.apply   (lambda x: pad_zero(x, 2))
    df.cep        = df.cep.apply       (lambda x: pad_zero(x, 8))
    df.branch     = df.branch.astype(bool)
    df.opening_date = df.opening_date.apply(int_to_date)
    df.closing_date = df.closing_date.apply(int_to_date)

    df.to_sql('business', conn, index=False, if_exists='append')

conn.commit()

# ======================================
# ⏱️ Measure query time BEFORE indexing
# ======================================
print()
print("🔍 Measuring query performance before indexing...")
print()

measure_query_time(cursor,
    "SELECT * FROM partners WHERE name_partner LIKE 'JOÃO%'",
    "partners.name_partner (before index)"
)

measure_query_time(cursor,
    "SELECT * FROM companies WHERE corporate_name LIKE 'SUPERMERCADO%'",
    "companies.corporate_name (before index)"
)

measure_query_time(cursor,
    "SELECT * FROM business WHERE trade_name LIKE 'PADARIA%'",
    "business.trade_name (before index)"
)

# ==========================
# 📌 Create regular indexes
# ==========================
print()
print("⚙️ Creating indexes...")
print()

cursor.execute('CREATE INDEX IF NOT EXISTS idx_partners_name_partner ON partners(name_partner)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_companies_corporate_name ON companies(corporate_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_business_trade_name ON business(trade_name)')

conn.commit()

# =====================================
# ⏱️ Measure query time AFTER indexing
# =====================================
print()
print("✅ Measuring query performance after indexing...")
print()

measure_query_time(cursor,
    "SELECT * FROM partners WHERE name_partner LIKE 'JOÃO%'",
    "partners.name_partner (after index)"
)

measure_query_time(cursor,
    "SELECT * FROM companies WHERE corporate_name LIKE 'SUPERMERCADO%'",
    "companies.corporate_name (after index)"
)

measure_query_time(cursor,
    "SELECT * FROM business WHERE trade_name LIKE 'PADARIA%'",
    "business.trade_name (after index)"
)


conn.close()

print()
print(f"✅ SQLite database created at: {SQLITE_PATH}")
