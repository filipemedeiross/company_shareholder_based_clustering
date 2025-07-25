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
```

### 📥 DATA INGESTION

The datasets must be downloaded and extracted by running the script `scripts/1_data_ingestion.py`. The ingestion process includes:

1. **Downloading** 10 files named `Empresas[0-9].zip`, which after extraction total approximately **4.59 GB** of company data.
2. **Downloading** 10 files named `Estabelecimentos[0-9].zip`, which after extraction total approximately **14.3 GB** of establishment-level data.
3. **Downloading** 10 files named `Socios[0-9].zip`, which after extraction total approximately **2.47 GB** of partner/shareholder data.
4. **Extracting and renaming** each file to a standardized CSV format and saving them in their respective directories.

This script automates the download and extraction of large ZIP files using multiprocessing, enabling efficient and reliable handling of high-volume data. Three parallel processes are responsible for downloading the ZIP files related to company data (`Empresas` and `Estabelecimentos`) and shareholders data (`Socios`), while a fourth process handles the extraction of these files. During extraction, each file is unzipped and renamed according to a standardized naming convention (e.g., `estabelecimentos0.csv`), and placed in the appropriate directory.

> ⚠️ **Note**: This multiprocessing approach greatly improves performance by reducing total processing time, minimizing I/O bottlenecks, and ensuring large datasets are handled efficiently and in an organized manner. However, due to the substantial size of the data, the full download and extraction process may still take a significant amount of time. Patience is recommended—especially on slower internet connections or machines with limited disk throughput.

### 🔄 DATA TRANSFORMATION

After downloading and extracting the raw CSV files, you can transform the data into optimized and standardized Parquet files by running the script `scripts/2_data_transform.py`. This transformation is performed in four main steps and ensures that the final datasets are clean, well-structured, and ready for analysis or integration into data pipelines.

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
