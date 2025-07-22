import csv
import unittest
import pandas as pd

from pathlib import Path


class TestParquet(unittest.TestCase):
    ROOT_DIR = Path(__file__).resolve().parent.parent

    PARTNERS_PARQUET      =  ROOT_DIR / 'data/parquet/partners.parquet'
    BUSINESS_PARQUET      =  ROOT_DIR / 'data/parquet/business.parquet'
    SOCIOS_PATH           = [
        ROOT_DIR / f'data/csv/socios/socios{i}.csv'
        for i in range(10)
    ]
    ESTABELECIMENTOS_PATH = [
        ROOT_DIR / f'data/csv/estabelecimentos/estabelecimentos{i}.csv'
        for i in range(10)
    ]

    COLS_PARTNERS = [0, 2, 5]
    COLS_BUSINESS = [0, 4, 6, 10, 18]


    def test_partners_parquet_contains_csv_data(self):
        sample = pd.read_parquet(self.PARTNERS_PARQUET).sample(n=10)

        for _, (cnpj, partner, start) in sample.iterrows():
            row_found = False

            for csv_path in self.SOCIOS_PATH:
                with open(csv_path, encoding="latin-1", newline='') as f:
                    reader = csv.reader(f, delimiter=';', quotechar='"')

                    for row in reader:
                        if not row:
                            continue

                        try:
                            cnpj_csv, partner_csv, start_csv = row[0], row[2], row[5]
                        except IndexError:
                            continue

                        if cnpj == int(cnpj_csv) and \
                           partner_csv == name_partner and \
                        int(start_csv) == partnership_start:
                            print(cnpj_csv, partner_csv, start_csv)
                            print(cnpj, name_partner, partnership_start)
                            break

                if not match.empty:
                    print("\n✅ Match found!")
                    print(f"📁 In file: socios{i}.csv")
                    print("🔎 Row from parquet:")
                    print(row.to_frame().T.to_string(index=False))
                    print("📄 Matching row from CSV:")
                    print(match.iloc[0].to_frame().T.to_string(index=False))
                    row_found = True
                    break

            if not row_found:
                self.fail(
                    f"\n❌ Row from parquet not found in any socios[0-9].csv file:\n{row.to_frame().T.to_string(index=False)}"
                )


    def test_partners_parquet_contains_csv_data(self):
        df_parquet = pd.read_parquet(self.PARTNERS_PARQUET)

        for i in range(10):
            csv_path = self.SOCIOS_DIR / f'socios{i}.csv'
            df_csv = pd.read_csv(
                csv_path                  ,
                sep=';'                   ,
                usecols=self.COLS_PARTNERS,
                names=self.NAMES_PARTNERS ,
                encoding='latin-1'        ,
                on_bad_lines='skip'       ,
            ).dropna()

            df_csv.cnpj              = df_csv.cnpj.astype('int32')
            df_csv.partnership_start = pd.to_datetime(
                df_csv.partnership_start,
                format='%Y%m%d',
                errors='coerce',
            )

            row   = df_csv.sample(1).iloc[0]
            found = df_parquet[
                (df_parquet.cnpj              == row.cnpj              ) &
                (df_parquet.name_partner      == row.name_partner      ) &
                (df_parquet.partnership_start == row.partnership_start )
            ]

            self.assertFalse(
                found.empty,
                f'Missing row in partners.parquet from socios{i}.csv: {row.to_dict()}'
            )

            del df_csv

    def test_business_parquet_contains_csv_data(self):
        df_parquet = pd.read_parquet(self.BUSINESS_PARQUET)

        for i in range(10):
            csv_path = self.ESTABELECIMENTOS_DIR / f'estabelecimentos{i}.csv'
            df_csv = pd.read_csv(
                csv_path                  ,
                sep=';'                   ,
                usecols=self.COLS_BUSINESS,
                names=self.NAMES_BUSINESS ,
                encoding='latin-1'        ,
                on_bad_lines='skip'       ,
                low_memory=False          ,
            )

            df_csv.cep = (
                df_csv.cep
                .fillna('0')
                .astype(str)
                .str.replace('-', '', regex=True)
                .astype(float)
                .astype(int)
            )

            row   = df_csv.sample(1).iloc[0]
            found = df_parquet[
                (df_parquet.cnpj         == row.cnpj        ) &
                (df_parquet.trade_name   == row.trade_name  ) &
                (df_parquet.closing_date == row.closing_date) &
                (df_parquet.opening_date == row.opening_date) &
                (df_parquet.cep          == row.cep         )
            ]

            self.assertFalse(
                found.empty,
                f"Missing row in business.parquet from estabelecimentos{i}.csv: {row.to_dict()}"
            )

            del df_csv

if __name__ == '__main__':
    unittest.main()
