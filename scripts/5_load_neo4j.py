import os
import neo4j
import sqlite3

from .constants import SQLITE_PATH


ENV_VAR = "NEO4J_PASSWORD"


def get_secret_from_env(var_name):
    value = os.environ.get(var_name)

    if not value:
        raise RuntimeError(f"Environment variable {var_name!r} is not defined")

    return value

def import_partners(tx, cnpj, partner_name, start_date):
    query = """
        MERGE (c:Company {cnpj: $cnpj})

        MERGE (p:Partner {name: $partner_name})

        MERGE (p)-[r:PARTNER_OF]->(c)
        ON CREATE SET r.start_date = $start_date
        ON MATCH SET  r.start_date = coalesce(r.start_date, $start_date)
    """

    tx.run(
        query,
        cnpj=cnpj,
        partner_name=partner_name,
        start_date=start_date
    )


# ==============================
# 1. Connection to Neo4j Desktop
# ==============================
NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = get_secret_from_env(ENV_VAR)

driver = neo4j.GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# =============================
# 2. Connection to local SQLite
# =============================
conn   = sqlite3.connect(SQLITE_PATH)
cursor = conn.cursor()

# =========================================
# 3. Read from SQLite and insert into Neo4j
# =========================================
cursor.execute("""
    SELECT partners.cnpj, partners.name_partner, partners.start_date
    FROM partners
""")

rows = cursor.fetchall()
print(f"🔄 Inserting {len(rows)} relationships into Neo4j...")

with driver.session() as session:
    for cnpj, partner_name, start_date in rows:
        session.execute_write(import_partners, cnpj, partner_name, start_date)

print("✅ Graph created with Partner nodes, Company nodes, and PARTNER_OF relationships.")

# ====================
# 4. Close connections
# ====================
conn  .close()
driver.close()
