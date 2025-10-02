import time
import duckdb
import sqlite3
import unittest

from scripts.constants import DUCKDB_PATH, \
                              SQLITE_PATH


class TestDBPerformance(unittest.TestCase):
    def setUp(self):
        self.duckdb_conn = duckdb .connect(DUCKDB_PATH)
        self.sqlite_conn = sqlite3.connect(SQLITE_PATH)
        self.sqlite_conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.duckdb_conn.close()
        self.sqlite_conn.close()

    def _run_query_with_timing(self, db_type, query_func):
        start_time = time .time()
        result     = query_func()
        end_time   = time .time()
        execution_time = end_time - start_time

        print(f"{db_type} - Execution time: {execution_time:.6f} seconds")

        return result, execution_time

    def _get_duckdb_query(self, query):
        return lambda: self.duckdb_conn    \
                           .execute(query) \
                           .fetchdf()      \
                           .to_dict(orient="records")[0]

    def _get_sqlite_query(self, query):
        def run():
            cursor = self.sqlite_conn.cursor()
            cursor.execute(query)
            result = dict(cursor.fetchone())
            cursor.close()
            return result

        return run

    def _compare_and_report(self, entity_name, duckdb_query, sqlite_query, key):
        print()
        print(f"=== Testing {entity_name} statistics ===")

        duckdb_result, duckdb_time = self._run_query_with_timing("DuckDB", duckdb_query)
        sqlite_result, sqlite_time = self._run_query_with_timing("SQLite", sqlite_query)

        print(f"DuckDB is {sqlite_time / duckdb_time:.2f}x faster than SQLite for {entity_name} statistics")

        self.assertEqual(duckdb_result[key], sqlite_result[key])

    def test_companies_statistics(self):
        query = """
            SELECT 
                COUNT(DISTINCT cnpj)  AS total_companies,
                MIN(capital)          AS min_capital,
                MAX(capital)          AS max_capital,
                AVG(capital)          AS avg_capital
            FROM companies
        """

        self._compare_and_report(
            "companies",
            self._get_duckdb_query(query),
            self._get_sqlite_query(query),
            "total_companies"
        )

    def test_partners_statistics(self):
        query = """
            SELECT 
                COUNT(DISTINCT name_partner)  AS total_partners,
                MIN(start_date)               AS earliest_partner_date,
                MAX(start_date)               AS latest_partner_date
            FROM partners
        """

        self._compare_and_report(
            "partners",
            self._get_duckdb_query(query),
            self._get_sqlite_query(query),
            "total_partners"
        )

    def test_business_statistics(self):
        query = """
            SELECT 
                COUNT(*)              AS total_business,
                COUNT(DISTINCT cnpj)  AS unique_business,
                MIN(opening_date)     AS earliest_opening,
                MAX(opening_date)     AS latest_opening,
                COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
            FROM business
        """

        self._compare_and_report(
            "business",
            self._get_duckdb_query(query),
            self._get_sqlite_query(query),
            "total_business"
        )


if __name__ == '__main__':
    unittest.main()
