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


    def find_row_partners(
        self    ,
        files   ,
        cols    ,
        cnpj_col,
        name_col,
        target  ,
    ):
        for csv_path in files:
            with open(csv_path, encoding="latin-1", newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')

                for row in reader:
                    if (
                        int(row[cnpj_col]) == target.cnpj and
                            row[name_col]  == target.name_partner
                    ):
                        return [row[idx] for idx in cols]

        return None


    def find_row_business(
        self        ,
        files       ,
        cols        ,
        cnpj_col    ,
        closing_col ,
        opening_col ,
        target      ,
    ):
        for csv_path in files:
            with open(csv_path, encoding="latin-1", newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')

                for row in reader:
                    if (
                        int(row[cnpj_col    ]) == target.cnpj         and
                        int(row[closing_col ]) == target.closing_date and
                        int(row[opening_col ]) == target.opening_date
                    ):
                        return [row[idx] for idx in cols]

        return None


    def test_partners_parquet_contains_csv_data(self):
        CNPJ_COL, NAME_COL, _ = SOCIOS_COLS = [0, 2, 5]
        SOCIOS_PATH = [
            self.ROOT_DIR / f'data/csv/socios/socios{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        for _, row in sample.iterrows():
            found = self.find_row_partners(
                SOCIOS_PATH,
                SOCIOS_COLS,
                CNPJ_COL   ,
                NAME_COL   ,
                row        ,
            )

            cnpj, name, start_date = row
            with self.subTest(
                cnpj=cnpj       ,
                partner=name    ,
                start=start_date,
            ):
                if found:
                    print(
                        f"✅ Found match:\n"
                        f"Parquet:     {cnpj}, {name}, {start_date}\n"
                        f"CSV Match:   {found[0]}, {found[1]}, {found[2]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any socios[0-9].csv file."
                    )


    def test_business_parquet_contains_csv_data(self):
        CNPJ_COL    =  0
        CLOSING_COL =  6
        OPENING_COL = 10
        ESTABELECIMENTOS_COLS = [0, 4, 6, 10, 18]

        ESTABELECIMENTOS_PATH = [
            self.ROOT_DIR / f'data/csv/estabelecimentos/estabelecimentos{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.BUSINESS_PARQUET)

        for _, row in sample.iterrows():
            found = self.find_row_business(
                ESTABELECIMENTOS_PATH,
                ESTABELECIMENTOS_COLS,
                CNPJ_COL             ,
                CLOSING_COL          ,
                OPENING_COL          ,
                row                  ,
            )

            cnpj, trade_name, closing, opening, cep = row
            with self.subTest(
                cnpj=cnpj      ,
                name=trade_name,
                start=opening  ,
                end=closing    ,
                cep=cep        ,
            ):
                if found:
                    print(
                        f"✅ Found match:\n"
                        f"Parquet:     {cnpj}, {trade_name or ''}, {closing}, {opening}, {cep}\n"
                        f"CSV Match:   {found[0]}, {found[1]}, {found[2]}, {found[3]}, {found[4]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any estabelecimentos[0-9].csv file."
                    )


if __name__ == '__main__':
    unittest.main()
