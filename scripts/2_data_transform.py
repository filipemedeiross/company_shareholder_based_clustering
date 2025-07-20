import pandas as pd
from pathlib import Path


COLS  = [0, 2, 5]
NAMES = [
    'cnpj'             ,
    'name_partner'     ,
    'partnership_start',
]

ROOT_DIR    = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT_DIR / 'data/parquet/partners.parquet'


dataframes = []
for i in range(10):
    file_path = ROOT_DIR / f'data/csv/socios/socios{i}.csv'

    print('Reading', file_path)

    df = pd.read_csv(
        file_path          ,
        sep=';'            ,
        usecols=COLS       ,
        names=NAMES        ,
        encoding='latin-1' ,
        on_bad_lines='skip',
    )

    df.cnpj = df.cnpj.astype('int32')
    df.partnership_start = pd.to_datetime(df.partnership_start, format='%Y%m%d', errors='coerce')

    dataframes.append(df)

data = pd.concat(dataframes, ignore_index=True)
data = data.dropna()

data.to_parquet(OUTPUT_PATH, index=False)
