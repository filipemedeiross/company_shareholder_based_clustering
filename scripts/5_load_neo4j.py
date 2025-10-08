import os
import neo4j
import sqlite3

from .constants import SQLITE_PATH


def get_secret_from_env(var_name):
    value = os.environ.get(var_name)

    if not value:
        raise RuntimeError(f"Environment variable {var_name!r} is not defined")

    return value

def import_partners(tx, batch):
    query = """
        UNWIND $batch AS row

        MERGE (c:Company {cnpj: row[0]})
        ON CREATE SET c.corporate_name = row[2]
        ON MATCH  SET c.corporate_name = coalesce(c.corporate_name, row[2])

        MERGE (p:Partner {name: row[1]})
        MERGE (p)-[:PARTNER_OF]->(c)
    """

    tx.run(query, batch=batch)

def fetch_in_chunks(cursor, chunk_size=10_000):
    while rows := cursor.fetchmany(chunk_size):
        yield rows


# ===========================
# Connection to Neo4j Desktop
# ===========================
ENV_VAR = "NEO4J_PASSWORD"

NEO4J_URI  = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = get_secret_from_env(ENV_VAR)

driver = neo4j.GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

with driver.session() as session:
    session.run("CREATE CONSTRAINT company_cnpj IF NOT EXISTS FOR (c:Company) REQUIRE c.cnpj IS UNIQUE")
    session.run("CREATE CONSTRAINT partner_name IF NOT EXISTS FOR (p:Partner) REQUIRE p.name IS UNIQUE")

# ==========================
# Connection to local SQLite
# ==========================
conn   = sqlite3.connect(SQLITE_PATH)
cursor = conn.cursor()

cursor.execute("""
    SELECT 
        p.cnpj,
        p.name_partner,
        c.corporate_name
    FROM partners AS p
    JOIN companies AS c USING (cnpj)
""")

# =====================
# Insert data in chunks
# =====================
total_inserted = 0

with driver.session() as session:
    for chunk in fetch_in_chunks(cursor):
        session.execute_write(import_partners, chunk)

        total_inserted += len(chunk)
        if total_inserted % 500_000 == 0:
            print(f"✅ Inserted {total_inserted} relationships so far...")

print("✅ Graph created with Partner nodes, Company nodes, and PARTNER_OF relationships.")

# =================
# Close connections
# =================
conn  .close()
driver.close()
