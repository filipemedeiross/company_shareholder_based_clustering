import duckdb
from django.core.cache import cache

from scripts.constants    import DUCKDB_PATH
from summarizer.constants import CACHE_TIMEOUT


def get_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def get_statistics():
    if cached_stats := cache.get('dashboard_stats'):
        return cached_stats

    conn  = get_connection()
    stats = {}

    # =============
    # 📊 Companies
    # =============
    stats['companies'] = conn.execute("""
        SELECT 
            COUNT(DISTINCT cnpj) AS total_companies,
            MIN(capital)         AS min_capital,
            MAX(capital)         AS max_capital,
            AVG(capital)         AS avg_capital
        FROM companies
    """).fetchdf().to_dict(orient="records")[0]

    # ============
    # 📊 Partners
    # ============
    stats['partners'] = conn.execute("""
        SELECT 
            COUNT(DISTINCT name_partner) AS total_partners,
            MIN(start_date)              AS earliest_partner_date,
            MAX(start_date)              AS latest_partner_date
        FROM partners
    """).fetchdf().to_dict(orient="records")[0]

    # ============
    # 📊 Business
    # ============
    stats['business'] = conn.execute("""
        SELECT 
            COUNT(*)              AS total_business,
            COUNT(DISTINCT cnpj)  AS unique_business,
            MIN(opening_date)     AS earliest_opening,
            MAX(opening_date)     AS latest_opening,
            COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
        FROM business
    """).fetchdf().to_dict(orient="records")[0]

    cache.set('dashboard_stats', stats, CACHE_TIMEOUT)
    conn .close()

    return stats
