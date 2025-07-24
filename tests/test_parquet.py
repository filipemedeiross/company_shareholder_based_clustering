import csv
import random
import unittest
import pandas as pd

from pathlib     import Path
from fastparquet import ParquetFile


class TestParquet(unittest.TestCase):
    ROOT_DIR = Path(__file__).resolve().parent.parent

    PARTNERS_PARQUET = ROOT_DIR / 'data/parquet/partners.parquet'
    BUSINESS_PARQUET = ROOT_DIR / 'data/parquet/business.parquet'


    def get_sample_from_parquet(self, filename, n=10):
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


    def find_row(self, files, cols, cnpj_col, cnpj):
        for csv_path in files:
            with open(csv_path, encoding="latin-1", newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')

                for row in reader:
                    if not row:
                        continue

                    if int(row[cnpj_col]) == cnpj:
                        return [row[idx] for idx in cols]

        return None


    def test_partners_parquet_contains_csv_data(self):
        CNPJ_COL, *_ = SOCIOS_COLS = [0, 2, 5]
        SOCIOS_PATH = [
            self.ROOT_DIR / f'data/csv/socios/socios{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        for _, (cnpj, name, start_date) in sample.iterrows():
            row_found = self.find_row(
                SOCIOS_PATH,
                SOCIOS_COLS,
                CNPJ_COL   ,
                cnpj       ,
            )

            with self.subTest(cnpj=cnpj, partner=name, start=start_date):
                if row_found:
                    print(
                        f"✅ Found match:\n"
                        f"Parquet:     {cnpj}, {name}, {start_date}\n"
                        f"CSV Match:   {row_found[0]}, {row_found[1]}, {row_found[2]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any socios[0-9].csv file."
                    )


    def test_business_parquet_contains_csv_data(self):
        CNPJ_COL, *_ = ESTABELECIMENTOS_COLS = [0, 4, 6, 10, 18]
        ESTABELECIMENTOS_PATH = [
            self.ROOT_DIR / f'data/csv/estabelecimentos/estabelecimentos{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.BUSINESS_PARQUET)

        for _, (cnpj, trade_name, closing_date, opening_date, cep) in sample.iterrows():
            row_found = self.find_row(
                ESTABELECIMENTOS_PATH,
                ESTABELECIMENTOS_COLS,
                CNPJ_COL             ,
                cnpj                 ,
            )

            with self.subTest(cnpj=cnpj, name=trade_name, start=opening_date, end=closing_date, cep=cep):
                if row_found:
                    print(
                        f"✅ Found match:\n"
                        f"Parquet:     {cnpj}, {trade_name}, {opening_date}, {closing_date}, {cep}\n"
                        f"CSV Match:   {row_found[0]}, {row_found[1]}, {row_found[2]}, {row_found[3]}, {row_found[4]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any estabelecimentos[0-9].csv file."
                    )


if __name__ == '__main__':
    unittest.main()
