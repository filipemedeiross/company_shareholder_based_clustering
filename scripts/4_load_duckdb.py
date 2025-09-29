import duckdb

from .load import measure_query_time
from .constants import DATA_DIR   , \
                       SQLITE_PATH


DUCKDB_DIR  = DATA_DIR   / 'duckdb'
DUCKDB_PATH = DUCKDB_DIR / 'rfb.duckdb'

DUCKDB_DIR.mkdir(parents=True, exist_ok=True)


# ==============================
# 🔌 DuckDB setup and connection
# ==============================
print("🔌 Connecting to DuckDB database...")

conn = duckdb.connect(DUCKDB_PATH)

# ==================================
# 🚀 Load data directly from SQLite
# ==================================
print()
print("📥 Loading data from SQLite directly into DuckDB with automatic schema inference...")

conn.execute(f"DROP TABLE IF EXISTS companies")
conn.execute(f"CREATE TABLE companies AS SELECT * FROM sqlite_scan('{SQLITE_PATH}', 'companies')")

conn.execute(f"DROP TABLE IF EXISTS partners")
conn.execute(f"CREATE TABLE partners AS SELECT * FROM sqlite_scan('{SQLITE_PATH}', 'partners')")

conn.execute(f"DROP TABLE IF EXISTS business")
conn.execute(f"CREATE TABLE business AS SELECT * FROM sqlite_scan('{SQLITE_PATH}', 'business')")

# ============================
# 🔍 Measure query performance
# ============================
print()
print("🔍 Measuring query performance...")
print()

measure_query_time(
    conn,
    '''SELECT COUNT(*)
       FROM partners
       WHERE name_partner LIKE 'João%' ''',
    "partners.name_partner (before index)"
)

measure_query_time(
    conn,
    '''SELECT COUNT(*)
       FROM business
       WHERE trade_name LIKE 'Padaria%' ''',
    "business.trade_name (before index)"
)

measure_query_time(
    conn,
    '''SELECT COUNT(*)
       FROM companies
       WHERE corporate_name LIKE 'Comercio%' ''',
    "companies.corporate_name (before index)"
)

# ===================
# 💾 Optimize storage
# ===================
print()
print("💾 Optimizing database storage...")

conn.execute('VACUUM')

# ==========
# 🧹 Finish
# ==========
conn.close()

print()
print(f"✅ DuckDB database successfully created at: {DUCKDB_PATH}")
print("✅ Data stored in columnar format optimized for analytics.")
