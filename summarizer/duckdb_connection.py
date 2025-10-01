import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DUCKDB_PATH = BASE_DIR / 'data' / 'duckdb' / 'rfb.duckdb'


def get_connection():
    """Retorna uma conexão com o banco DuckDB."""
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    return conn


def get_statistics():
    """Retorna estatísticas gerais do banco de dados (companies, partners, business)."""
    conn = get_connection()

    stats = {}

    # ========================
    # 📊 Estatísticas Companies
    # ========================
    stats['companies'] = conn.execute("""
        SELECT 
            COUNT(*)              AS total_companies,
            MIN(capital)          AS min_capital,
            MAX(capital)          AS max_capital,
            AVG(capital)          AS avg_capital
        FROM companies
    """).fetchdf().to_dict(orient="records")[0]

    # ======================
    # 📊 Estatísticas Partners
    # ======================
    stats['partners'] = conn.execute("""
        SELECT 
            COUNT(*)              AS total_partners,
            MIN(start_date)       AS earliest_partner_date,
            MAX(start_date)       AS latest_partner_date
        FROM partners
    """).fetchdf().to_dict(orient="records")[0]

    # ======================
    # 📊 Estatísticas Business
    # ======================
    stats['business'] = conn.execute("""
        SELECT 
            COUNT(*)              AS total_business,
            COUNT(DISTINCT cnpj)  AS unique_companies,
            MIN(opening_date)     AS earliest_opening,
            MAX(opening_date)     AS latest_opening,
            COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
        FROM business
    """).fetchdf().to_dict(orient="records")[0]

    conn.close()
    return stats


if __name__ == "__main__":
    summary = get_statistics()
    from pprint import pprint
    pprint(summary)
