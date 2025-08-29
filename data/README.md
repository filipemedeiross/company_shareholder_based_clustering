## 📁 DATA

This project uses public datasets provided by the [Receita Federal do Brasil (RFB)](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/), which contain detailed information on Brazilian companies, their physical establishments, and their ownership structures.

### 📂 DATA ORGANIZATION

```
data/
    README.md

    csv/                            # raw data obtained from the RFB
        empresas/                   # information on the legal entity (company-level)
            empresas0.csv
            [...]
            empresas9.csv
        estabelecimentos/           # physical units such as branches
            estabelecimentos0.csv
            [...]
            estabelecimentos9.csv
        socios/                     # ownership structure (shareholders/partners)
            socios0.csv
            [...]
            socios9.csv
    parquet/
        business.parquet
        companies.parquet
        partners.parquet
    sqlite/
        db.sqlite3
        rfb.sqlite3
```

### 📥 DATA INGESTION

The datasets must be downloaded and extracted by running the script `scripts/1_ingestion.py`. The ingestion process includes:

1. **Downloading** 10 files named `Empresas[0-9].zip`, which after extraction total approximately **4.59 GB** of company data.
2. **Downloading** 10 files named `Estabelecimentos[0-9].zip`, which after extraction total approximately **14.3 GB** of establishment-level data.
3. **Downloading** 10 files named `Socios[0-9].zip`, which after extraction total approximately **2.47 GB** of partner/shareholder data.
4. **Extracting and renaming** each file to a standardized CSV format and saving them in their respective directories.

This script automates the download and extraction of large ZIP files using multiprocessing, enabling efficient and reliable handling of high-volume data. Three parallel processes are responsible for downloading the ZIP files related to company data (`Empresas` and `Estabelecimentos`) and shareholders data (`Socios`), while a fourth process handles the extraction of these files. During extraction, each file is unzipped and renamed according to a standardized naming convention (e.g., `estabelecimentos0.csv`), and placed in the appropriate directory.

> ⚠️ **Note**: This multiprocessing approach greatly improves performance by reducing total processing time, minimizing I/O bottlenecks, and ensuring large datasets are handled efficiently and in an organized manner. However, due to the substantial size of the data, the full download and extraction process may still take a significant amount of time. Patience is recommended—especially on slower internet connections or machines with limited disk throughput.

### 🔄 DATA TRANSFORMATION

After downloading and extracting the raw CSV files, you can transform the data into optimized and standardized Parquet files by running the script `scripts/2_transform.py`. This transformation is performed in four main steps and ensures that the final datasets are clean, well-structured, and ready for analysis or integration into data pipelines.

1. **Column selection and renaming**: Only relevant columns are kept from the original CSV files, and are renamed to standardized names.
2. **Data type conversion**: Numeric fields are explicitly cast to appropriate types (e.g., `int8`, `int16`) to reduce memory usage and ensure consistency.
3. **Incremental Parquet generation**: Each CSV file is read in chunks (when applicable) and written incrementally using the `fastparquet` engine, producing:
   - `partners.parquet` for ownership data (from `socios/`)
   - `companies.parquet` for legal entities (from `empresas/`)
   - `business.parquet` for physical establishments (from `estabelecimentos/`)
4. **Post-processing and compression**: The resulting Parquet files are optionally reloaded, sorted by relevant fields (e.g., `start_date`, `opening_date`, `cep`), and rewritten using `pyarrow` for better compression.

> ✅ **Note**: You can validate the integrity and consistency of these transformations by running the test suite available in `tests/test_parquet.py`. This suite checks whether randomly sampled rows from the Parquet files can be traced back to their original CSV entries, and verifies the overlap of CNPJs across the transformed datasets.
>
> Run the tests with:
> 
>```bash
>python -m unittest tests.test_parquet.py
>```

### 🗃️ SQLITE DATABASE

Run `scripts/3_load_sqlite.py` to create a local SQLite database by loading the Parquet files into three structured tables:

- `partners (cnpj, name_partner, start_date)`
- `companies(cnpj, corporate_name, capital)`
- `business (cnpj, cnpj_order, cnpj_dv, branch, trade_name, closing_date, opening_date, cep)`

After execution, you’ll have a lightweight SQLite database at `data/sqlite/rfb.sqlite3`, offering enhanced performance for prototyping, searching, and exploratory analysis. The data is inserted efficiently by processing each row group individually, and duplicate entries in the `companies` table are removed based on the `cnpj` field.

To enhance query performance on textual columns (`name_partner`, `corporate_name` and `trade_name`), the script also creates **FTS5 virtual tables**:

- `partners_fts (name_partner)`
- `companies_fts(corporate_name)`
- `business_fts (trade_name)`

These indexes enable fast full-text searches using the `MATCH` operator, which significantly outperforms standard `LIKE` queries—especially for prefix-based searches. The chart below illustrates the performance difference between `LIKE` and `FTS5 MATCH` on the columns name_partner and trade_name:

![FTS5 vs LIKE performance](https://github.com/filipemedeiross/company_shareholder_based_clustering/blob/main/docs/tfs5/fts5_vs_like.png?raw=true)

> ✅ **Note**: You can validate the integrity and performance of the SQLite database using:
>
>```bash
>python -m unittest tests.test_sqlite
>```
>
> The test suite includes:
>
> - 🔍 Existence checks: Ensures entries from the Parquet files exist in the partners, companies, and business tables.
> - 🔎 FTS5 validation: Confirms prefix-based matches using full-text search on name_partner and trade_name.
> - ⏱️ Performance comparison: Benchmarks LIKE vs FTS5 MATCH to demonstrate the efficiency of full-text indexing.
>
> ℹ️ **Integration with Django Hermes**:
>
> The `rfb.sqlite3` database is used in **read-only mode** by the **companies** app of the django project **Hermes**, enabling fast queries over company and partner data. Other application data is stored separately in `data/sqlite/db.sqlite3`.  
>  
> Database routing between these two files is handled by the **companies database router**, ensuring that queries related to RFB data are directed exclusively to `rfb.sqlite3`.  
>  
> You can also test the django models mapped to the RFB tables by running:  
> ```bash
> python manage.py test companies.tests.test_orm
> ```
