import sqlite3

from .load import insert_parquet    , \
                  measure_query_time
from .constants import SQLITE_PATH      , \
                       PARQUET_PARTNERS , \
                       PARQUET_COMPANIES, \
                       PARQUET_BUSINESS


SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)


# ==========================
# 🔌 SQLite setup and tables
# ==========================
print("🔌 Connecting to SQLite database and creating tables...")

conn   = sqlite3.connect(SQLITE_PATH)
cursor = conn.cursor()

cursor.execute('PRAGMA foreign_keys = ON')

cursor.execute('DROP TABLE IF EXISTS companies')
cursor.execute('''
    CREATE TABLE companies (
        cnpj TEXT PRIMARY KEY,
        corporate_name TEXT,
        capital INTEGER
    )
''')

cursor.execute('DROP TABLE IF EXISTS partners')
cursor.execute('''
    CREATE TABLE partners (
        cnpj TEXT,
        name_partner TEXT,
        start_date TEXT,
        FOREIGN KEY (cnpj) REFERENCES companies(cnpj)
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
        closing_date TEXT,
        opening_date TEXT,
        cep TEXT,
        PRIMARY KEY (cnpj, cnpj_order, cnpj_dv),
        FOREIGN KEY (cnpj) REFERENCES companies(cnpj)
    )
''')

conn.commit()

# ===========================
# 🚀 Load and insert all data
# ===========================
insert_parquet(
    conn             ,
    'companies'      ,
    PARQUET_COMPANIES,
    duplicates='cnpj',
)
insert_parquet(conn, 'partners' , PARQUET_PARTNERS)
insert_parquet(conn, 'business' , PARQUET_BUSINESS)

conn.commit()

# ==========================
# 🔍 Measure before indexing
# ==========================
print()
print("🔍 Measuring query performance before indexing...")
print()

measure_query_time(
    cursor,
    '''SELECT *
       FROM partners
       WHERE start_date >= '2020-01-01' AND start_date < '2021-01-01' ''',
    "partners.start_date (before index)"
)

measure_query_time(
    cursor,
    '''SELECT *
       FROM business
       WHERE opening_date >= '2020-01-01' AND closing_date <= '2021-12-31' ''',
    "business.opening_date + closing_date (before index)"
)

measure_query_time(
    cursor,
    '''SELECT name_partner
       FROM partners
       WHERE name_partner LIKE 'João%' ''',
    "partners.name_partner (before index)"
)

measure_query_time(
    cursor,
    '''SELECT trade_name
       FROM business
       WHERE trade_name LIKE 'Padaria%' ''',
    "business.trade_name (before index)"
)

# =========================================
# 📌 Create indexes and FTS5 virtual tables
# =========================================
print()
print("⚙️ Creating indexes and FTS5 virtual tables for text search...")
print()

cursor.execute('CREATE INDEX IF NOT EXISTS idx_partners_start_date ON partners(start_date)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_business_opening_closing ON business(opening_date, closing_date)')

cursor.execute('DROP TABLE IF EXISTS partners_fts')
cursor.execute('CREATE VIRTUAL TABLE partners_fts USING fts5(name_partner, content="partners", content_rowid="rowid")')
cursor.execute('INSERT INTO partners_fts(rowid, name_partner) SELECT rowid, name_partner FROM partners')

cursor.execute('DROP TABLE IF EXISTS business_fts')
cursor.execute('CREATE VIRTUAL TABLE business_fts USING fts5(trade_name, content="business", content_rowid="rowid")')
cursor.execute('INSERT INTO business_fts(rowid, trade_name) SELECT rowid, trade_name FROM business')

conn.commit()

# =========================
# ✅ Measure after indexing
# =========================
print()
print("✅ Measuring query performance using FTS5 indexes...")
print()

measure_query_time(
    cursor,
    '''SELECT *
       FROM partners
       WHERE start_date >= '2020-01-01' AND start_date < '2021-01-01' ''',
    "partners.start_date (after index)"
)

measure_query_time(
    cursor,
    '''SELECT *
       FROM business
       WHERE opening_date >= '2020-01-01' AND closing_date <= '2021-12-31' ''',
    "business.opening_date + closing_date (after index)"
)

measure_query_time(
    cursor,
    '''SELECT name_partner
       FROM partners_fts
       WHERE partners_fts MATCH 'João*' ''',
    "partners.name_partner (FTS5)"
)

measure_query_time(
    cursor,
    '''SELECT trade_name
       FROM business_fts
       WHERE business_fts MATCH 'Padaria*' ''',
    "business.trade_name (FTS5)"
)

# ===========
# 🧹 Finalize
# ===========
conn.close()

print()
print(f"✅ SQLite database successfully created at: {SQLITE_PATH}")
