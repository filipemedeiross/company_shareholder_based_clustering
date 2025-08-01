import random
import sqlite3
import unittest
import pandas as pd

from pathlib import Path
from fastparquet import ParquetFile


class TestSQLite(unittest.TestCase):
    ROOT_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = ROOT_DIR / 'data'

    SQLITE_DB         = DATA_DIR / 'sqlite/rfb.db'
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

        for _, (
            cnpj        ,
            cnpj_order  ,
            cnpj_dv     ,
            branch      ,
            trade_name  ,
            closing_date,
            opening_date,
            cep         ,
        ) in sample.iterrows():
            with self.subTest(cnpj=cnpj, order=cnpj_order, dv=cnpj_dv):
                self.cursor.execute(
                    '''
                    SELECT * FROM business
                    WHERE cnpj = ? AND cnpj_order = ? AND cnpj_dv = ?
                    ''',
                    (cnpj, cnpj_order, cnpj_dv)
                )
                results = self.cursor.fetchall()

                if not results:
                    self.fail(
                        f"❌ Entry not found in 'business' for CNPJ {cnpj}, order {cnpj_order}, dv {cnpj_dv}"
                    )
                elif len(results) > 1:
                    self.fail(
                        f"❌ More than one match found in 'business' for CNPJ {cnpj}, order {cnpj_order}, dv {cnpj_dv}"
                    )
                else:
                    result = results[0]

                    print(
                        f"✅ Found match:\n"
                        f"Parquet:      {cnpj     }, {cnpj_order}, {cnpj_dv  }\n"
                        f"SQLite Match: {result[0]}, {result[1] }, {result[2]}"
                    )
