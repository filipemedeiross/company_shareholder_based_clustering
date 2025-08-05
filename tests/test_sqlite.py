import time
import random
import sqlite3
import unittest

import pandas as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
import matplotlib.pyplot as plt

from pathlib import Path
from fastparquet import ParquetFile


class TestSQLiteBase(unittest.TestCase):
    ROOT_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = ROOT_DIR / 'data'

    SQLITE_DB         = DATA_DIR / 'sqlite/rfb.sqlite3'
    PARTNERS_PARQUET  = DATA_DIR / 'parquet/partners.parquet'
    COMPANIES_PARQUET = DATA_DIR / 'parquet/companies.parquet'
    BUSINESS_PARQUET  = DATA_DIR / 'parquet/business.parquet'


    def setUp(self):
        self.conn   = sqlite3.connect(self.SQLITE_DB)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def get_sample_from_parquet(self, filename, n=3):
        samples = []

        with open(filename, mode='rb') as f:
            pf   = ParquetFile(f)
            idxs = sorted(random.sample(range(pf.count()), n))

            current_row = 0
            for rg in pf.iter_row_groups():
                num_rows = len(rg)

                local_indices = [
                    i - current_row
                    for i in idxs
                    if current_row <= i < current_row + num_rows
                ]

                if local_indices:
                    samples.append(rg.iloc[local_indices])

                current_row += num_rows
                if current_row > idxs[-1]:
                    break

        return pd.concat(samples, ignore_index=True)

    def get_sample_trade_name(self, filename, n=3):
        table = pq.read_table(filename, columns=['trade_name'])
        table = table.filter(pc.field('trade_name').is_valid())

        return table.to_pandas().sample(n)

    def time_query(self, query, params):
        start = time.time()
        self.cursor.execute(query, params)
        self.cursor.fetchall()

        return time.time() - start


class TestSQLite(TestSQLiteBase):
    def test_partners_exist_in_sqlite(self):
        print()
        print("▶️ Starting test: test_partners_exist_in_sqlite")
        print()

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        for _, (cnpj, partner, date) in sample.iterrows():
            with self.subTest(cnpj=cnpj):
                self.cursor.execute(
                    '''
                    SELECT * FROM partners
                    WHERE cnpj = ? AND name_partner = ? AND start_date = ?
                    ''',
                    (cnpj, partner, date)
                )
                results = self.cursor.fetchall()

                if not results:
                    self.fail(
                        f"❌ Entry not found in 'partners' for CNPJ {cnpj}, name {partner}, start_date {date}"
                    )
                elif len(results) > 1:
                    self.fail(
                        f"❌ More than one match found in 'partners' for CNPJ {cnpj}, name {partner}, start_date {date}"
                    )
                else:
                    result = results[0]

                    print(
                        f"✅ Found match:\n"
                        f"Parquet:      {cnpj     }, {partner  }, {date     }\n"
                        f"SQLite Match: {result[0]}, {result[1]}, {result[2]}"
                    )

    def test_companies_exist_in_sqlite(self):
        print()
        print("▶️ Starting test: test_companies_exist_in_sqlite")
        print()

        sample = self.get_sample_from_parquet(self.COMPANIES_PARQUET)

        for _, (cnpj, name, capital) in sample.iterrows():
            with self.subTest(cnpj=cnpj):
                self.cursor.execute(
                    '''
                    SELECT * FROM companies
                    WHERE cnpj = ? AND corporate_name = ? AND capital = ?
                    ''',
                    (cnpj, name, capital)
                )
                results = self.cursor.fetchall()

                if not results:
                    self.fail(
                        f"❌ Entry not found in 'companies' for CNPJ {cnpj}, name {name}, capital {capital}"
                    )
                elif len(results) > 1:
                    self.fail(
                        f"❌ More than one match found in 'companies' for CNPJ {cnpj}, name {name}, capital {capital}"
                    )
                else:
                    result = results[0]

                    print(
                        f"✅ Found match:\n"
                        f"Parquet:      {cnpj     }, {name     }, {capital  }\n"
                        f"SQLite Match: {result[0]}, {result[1]}, {result[2]}"
                    )

    def test_business_exist_in_sqlite(self):
        print()
        print("▶️ Starting test: test_business_exist_in_sqlite")
        print()

        sample = self.get_sample_from_parquet(self.BUSINESS_PARQUET)

        for _, (cnpj, order, dv, *_) in sample.iterrows():
            with self.subTest(cnpj=cnpj, order=order, dv=dv):
                self.cursor.execute(
                    '''
                    SELECT * FROM business
                    WHERE cnpj = ? AND cnpj_order = ? AND cnpj_dv = ?
                    ''',
                    (cnpj, order, dv)
                )
                results = self.cursor.fetchall()

                if not results:
                    self.fail(
                        f"❌ Entry not found in 'business' for CNPJ {cnpj}, order {order}, dv {dv}"
                    )
                elif len(results) > 1:
                    self.fail(
                        f"❌ More than one match found in 'business' for CNPJ {cnpj}, order {order}, dv {dv}"
                    )
                else:
                    result = results[0]

                    print(
                        f"✅ Found match:\n"
                        f"Parquet:      {cnpj     }, {order    }, {dv       }\n"
                        f"SQLite Match: {result[0]}, {result[1]}, {result[2]}"
                    )

    def test_fts_name_partner(self):
        print()
        print("🔎 Testing FTS5 on 'partners_fts.name_partner'")
        print()

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        for _, (_, name, _) in sample.iterrows():
            prefix = name.split()[0]
            match  = f"{prefix}*"

            self.cursor.execute(
                '''
                SELECT rowid FROM partners_fts
                WHERE partners_fts MATCH ?
                ORDER BY RANDOM()
                LIMIT 10
                ''',
                (match,)
            )
            rowids = [r[0] for r in self.cursor.fetchall()]

            if not rowids:
                self.fail(f"❌ No FTS match found for prefix '{match}'")

            self.cursor.execute(
                f'''
                SELECT name_partner FROM partners
                WHERE rowid IN ({','.join(['?'] * len(rowids))})
                ''',
                rowids
            )
            names = [r[0] for r in self.cursor.fetchall()]

            if not all(
                any(w.startswith(prefix) for w in name.split())
                for name in names
            ):
                self.fail(f"❌ RowIDs found, but not all start with the prefix '{prefix}'")
            else:
                print(f"✅ FTS5 match success")
                print(f"🔹 Full name sampled: {name}")
                print(f"🔹 First name used as prefix: '{prefix}'")
                print( "🔹 Matching names returned from SQLite:")

                for name in names:
                    print(f"   • {name}")

    def test_fts_trade_name(self):
        print()
        print("🔎 Testing FTS5 on 'business_fts.trade_name'")
        print()

        sample = self.get_sample_trade_name(self.BUSINESS_PARQUET)

        for trade_name in sample.trade_name:
            prefix = trade_name.split()[0]
            match  = f'"{prefix}"'

            self.cursor.execute(
                '''
                SELECT rowid FROM business_fts
                WHERE business_fts MATCH ?
                ORDER BY RANDOM()
                LIMIT 10
                ''',
                (match,)
            )
            rowids = [r[0] for r in self.cursor.fetchall()]

            if not rowids:
                self.fail(f"❌ No FTS match found for prefix '{match}'")

            self.cursor.execute(
                f'''
                SELECT trade_name FROM business
                WHERE rowid IN ({','.join(['?'] * len(rowids))})
                ''',
                rowids
            )
            names = [r[0] for r in self.cursor.fetchall()]

            if not all(
                prefix.upper() in name.upper()
                for name in names
            ):
                print(f"🔹 Full trade name sampled: {trade_name}")
                print(f"🔹 First name used as prefix: '{prefix}'")

                for name in names:
                    if prefix.upper() not in name.upper():
                        print(f"   • {name}")

                self.fail(f"❌ RowIDs found, but not all start with the prefix '{prefix}'")
            else:
                print(f"✅ FTS5 match success")
                print(f"🔹 Full trade name sampled: {trade_name}")
                print(f"🔹 First name used as prefix: '{prefix}'")
                print( "🔹 Matching trade names returned from SQLite:")

                for name in names:
                    print(f"   • {name}")


class TestSQLiteQueries(TestSQLiteBase):
    def test_query_time_fts_name_partner(self):
        print()
        print("⏱️ Comparing query times: 'name_partner' vs FTS5")
        print()

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        times_like = []
        times_fts  = []
        for _, (_, name, _) in sample.iterrows():
            prefix = name.split()[0]

            t1 = self.time_query(
                """
                SELECT rowid
                FROM partners
                WHERE name_partner LIKE ?
                """,
                (f"{prefix}%",)
            )
            t2 = self.time_query(
                """
                SELECT rowid
                FROM partners_fts
                WHERE partners_fts MATCH ?
                """,
                (f"{prefix}*",)
            )

            times_like.append(t1)
            times_fts.append (t2)

        self.avg_lp = sum(times_like) / len(times_like)
        self.avg_fp = sum(times_fts ) / len(times_fts )

        if self.avg_lp < self.avg_fp:
            self.fail("❌ FTS5 query is slower than direct LIKE query on the base table")
        else:
            print(f"📊 Average LIKE time for 'name_partner': {self.avg_lp:.2f}s")
            print(f"📊 Average FTS5 time for 'name_partner': {self.avg_fp:.2f}s")

    def test_query_time_fts_trade_name(self):
        print()
        print("⏱️ Comparing query times: 'trade_name' vs FTS5")
        print()

        sample = self.get_sample_trade_name(self.BUSINESS_PARQUET)

        times_like = []
        times_fts  = []
        for trade_name in sample.trade_name:
            prefix = trade_name.split()[0]

            t1 = self.time_query(
                """
                SELECT trade_name
                FROM business
                WHERE trade_name LIKE ?
                """,
                (f"{prefix}%",)
            )
            t2 = self.time_query(
                """
                SELECT rowid
                FROM business_fts
                WHERE business_fts MATCH ?
                """,
                (f"{prefix}*",)
            )

            times_like.append(t1)
            times_fts.append (t2)

        self.avg_lt = sum(times_like) / len(times_like)
        self.avg_ft = sum(times_fts ) / len(times_fts )

        if self.avg_lt < self.avg_ft:
            self.fail("❌ FTS5 query is slower than direct LIKE query on the base table")
        else:
            print(f"📊 Average LIKE time for 'trade_name': {self.avg_lt:.2f}s")
            print(f"📊 Average FTS5 time for 'trade_name': {self.avg_ft:.2f}s")

    def test_report_generate_fts5_vs_like_chart(self):
        print()
        print("📈 Generating FTS5 vs LIKE performance chart...")
        print()

        OUTPUT_PATH = self.ROOT_DIR / 'docs/tfs5'
        OUTPUT_FILE = OUTPUT_PATH   / 'fts5_vs_like.png'

        if OUTPUT_FILE.exists():
            print(f"⚠️ Chart already exists at: {OUTPUT_FILE}, skipping generation.")
            return

        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

        self.test_query_time_fts_name_partner()
        self.test_query_time_fts_trade_name  ()

        labels = ['LIKE', 'FTS5']
        partner_times = [self.avg_lp, self.avg_fp]
        trade_times   = [self.avg_lt, self.avg_ft]
        x = range(len(labels))

        fig, axs = plt.subplots(1, 2, figsize=(10, 5))
        fig.suptitle("Query Time Comparison", fontsize=14)

        axs[0].bar(
            x                               ,
            partner_times                   ,
            color=['#FF9999', '#99CCFF'],
        )
        axs[0].set_title("partners.name_partner")
        axs[0].set_ylabel("Average Time (s)")
        axs[0].set_xticks(x)
        axs[0].set_xticklabels(labels)

        axs[1].bar(
            x                               ,
            trade_times                     ,
            color=['#FF9999', '#99CCFF'],
        )
        axs[1].set_title("business.trade_name")
        axs[1].set_xticks(x)
        axs[1].set_xticklabels(labels)

        plt.tight_layout()
        plt.savefig(OUTPUT_FILE)

        print(f"✅ Chart saved at: {OUTPUT_PATH}")
