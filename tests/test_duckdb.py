import random
import duckdb
import sqlite3
import unittest

from scripts.constants import DUCKDB_PATH, \
                              SQLITE_PATH


class TestDuckDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sqlite_conn = sqlite3.connect(SQLITE_PATH)
        cls.duckdb_conn = duckdb .connect(DUCKDB_PATH)

        cls.tables = [
            "business",
            "partners",
            "companies"
        ]

    @classmethod
    def tearDownClass(cls):
        cls.sqlite_conn.close()
        cls.duckdb_conn.close()

    def get_random_rows(self, table, count=3):
        cursor = self.sqlite_conn.cursor()
        total  = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        return [
            cursor.execute(
                f"SELECT * FROM {table} LIMIT 1 OFFSET ?",
                (random.randint(0, total - 1),),
            ).fetchone()
            for _ in range(count)
        ]

    def row_exists_in_duckdb(self, table, row):
        cursor = self.sqlite_conn.cursor()

        columns = [
            col[1]
            for col in cursor.execute(f"PRAGMA table_info({table})")
        ]

        where = []
        for col, val in zip(columns, row):
            if val is None:
                where.append(f"{col} IS NULL")
            else:
                where.append(f"{col} = ?")

        result = self.duckdb_conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {' AND '.join(where)}",
            [v for v in row if v is not None]
        ).fetchone()[0]

        return result

    def test_data_consistency(self):
        for table in self.tables:
            with self.subTest(table=table):
                for row in self.get_random_rows(table):
                    self.assertEqual(self.row_exists_in_duckdb(table, row), 1)


if __name__ == '__main__':
    unittest.main()
