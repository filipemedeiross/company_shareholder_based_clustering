from pathlib import Path


BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-07/'


ROOT_DIR = Path(__file__).resolve().parent.parent

TMP_DIR     = ROOT_DIR / 'tmp'
DATA_DIR    = ROOT_DIR / 'data'
CSV_DIR     = DATA_DIR / 'csv'
PARQUET_DIR = DATA_DIR / 'parquet'

DICT_DIR = {
    'Socios'          : CSV_DIR / 'socios'          ,
    'Empresas'        : CSV_DIR / 'empresas'        ,
    'Estabelecimentos': CSV_DIR / 'estabelecimentos',
}

SQLITE_PATH = DATA_DIR / 'sqlite/rfb.sqlite3'
PARQUET_PARTNERS  = PARQUET_DIR / 'partners.parquet'
PARQUET_COMPANIES = PARQUET_DIR / 'companies.parquet'
PARQUET_BUSINESS  = PARQUET_DIR / 'business.parquet'


COLS_PARTNERS = [
    0, 2, 5
]
COLS_COMPANIES = [
    0, 1, 4
]
COLS_BUSINESS = [
    0, 1, 2, 3, 4, 6, 10, 18
]

NAMES_PARTNERS  = [
    'cnpj'        ,
    'name_partner',
    'start_date'  ,
]
NAMES_COMPANIES = [
    'cnpj'          ,
    'corporate_name',
    'capital'       ,
]
NAMES_BUSINESS  = [
    'cnpj'        ,
    'cnpj_order'  ,
    'cnpj_dv'     ,
    'branch'      ,
    'trade_name'  ,
    'closing_date',
    'opening_date',
    'cep'         ,
]

SORT_PARTNERS = [
    'start_date'  ,
    'name_partner',
]
SORT_COMPANIES = [
    'capital',
    'cnpj'   ,
]
SORT_BUSINESS = [
    'closing_date',
    'opening_date',
    'cep'         ,
]

CHUNKSIZE = 1_500_000
