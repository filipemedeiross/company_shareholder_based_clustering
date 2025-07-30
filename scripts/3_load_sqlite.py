import time
import sqlite3
import pyarrow.parquet as pq

from pathlib import Path


ROOT_DIR     = Path(__file__).resolve().parent.parent
PARQUET_DIR  = ROOT_DIR / 'data/parquet'
SQLITE_PATH  = ROOT_DIR / 'data/sqlite/rfb.db'

PARQUET_PARTNERS  = PARQUET_DIR / 'partners.parquet'
PARQUET_COMPANIES = PARQUET_DIR / 'companies.parquet'
PARQUET_BUSINESS  = PARQUET_DIR / 'business.parquet'

SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)


def insert_parquet(
    table_name  ,
    parquet_file,
    fn=None     ,
):
    print()
    print(f"📥 Loading and inserting data into '{table_name}' from Parquet (by row group)...")

    table = pq.ParquetFile(parquet_file)
    for i in range(table.num_row_groups):
        print(f"  • Processing row_group {i + 1}...")

        df = table.read_row_group(i).to_pandas()
        if fn:
            df = fn(df)

        df.to_sql(
            table_name        ,
            conn              ,
            index=False       ,
            if_exists='append',
        )
        del df

def transform_business(df):
    return df.branch.astype(bool)

def measure_query_time(cursor, query, label):
    start = time.perf_counter()
    cursor.execute(query).fetchall()
    end   = time.perf_counter()

    print(f"⏱️ Query time for {label}: {end - start:.2f} seconds")


# ==========================
# 🔌 SQLite setup and tables
# ==========================
print("🔌 Connecting to SQLite database and creating tables...")

conn   = sqlite3.connect(SQLITE_PATH)
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS partners')
cursor.execute('''
    CREATE TABLE partners (
        cnpj INTEGER,
        name_partner TEXT,
        start_date INTEGER
    )
''')

cursor.execute('DROP TABLE IF EXISTS companies')
cursor.execute('''
    CREATE TABLE companies (
        cnpj INTEGER,
        corporate_name TEXT,
        capital REAL
    )
''')

cursor.execute('DROP TABLE IF EXISTS business')
cursor.execute('''
    CREATE TABLE business (
        cnpj INTEGER,
        cnpj_order INTEGER,
        cnpj_dv INTEGER,
        branch BOOLEAN,
        trade_name TEXT,
        closing_date INTEGER,
        opening_date INTEGER,
        cep INTEGER
    )
''')

conn.commit()

# ===========================
# 🚀 Load and insert all data
# ===========================
insert_parquet('partners' , PARQUET_PARTNERS )
insert_parquet('companies', PARQUET_COMPANIES)
insert_parquet('business' , PARQUET_BUSINESS, fn=transform_business)

conn.commit()

# ==========================
# 🔍 Measure before indexing
# ==========================
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

# =================
# 📌 Create indexes
# =================
print()
print("⚙️ Creating indexes...")
print()

cursor.execute('CREATE INDEX IF NOT EXISTS idx_partners_name_partner ON partners(name_partner)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_companies_corporate_name ON companies(corporate_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_business_trade_name ON business(trade_name)')

conn.commit()

# =========================
# ✅ Measure after indexing
# =========================
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

# ===========
# 🧹 Finalize
# ===========
conn.close()

print()
print(f"✅ SQLite database successfully created at: {SQLITE_PATH}")
