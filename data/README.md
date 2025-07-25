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
        estabelecimentos/
            estabelecimentos0.csv   # physical units such as branches
            [...]
            estabelecimentos9.csv
        socios/                     # ownership structure (shareholders/partners)
            socios0.csv
            [...]
            socios9.csv
```

### 📥 DATA INGESTION

The datasets must be downloaded and extracted by running the script `scripts/1_data_ingestion.py`. The ingestion process includes:

1. **Downloading** 10 files named `Empresas[0-9].zip`, which after extraction total approximately **4.59 GB** of company data.
2. **Downloading** 10 files named `Estabelecimentos[0-9].zip`, which after extraction total approximately **14.3 GB** of establishment-level data.
3. **Downloading** 10 files named `Socios[0-9].zip`, which after extraction total approximately **2.47 GB** of partner/shareholder data.
4. **Extracting and renaming** each file to a standardized CSV format and saving them in their respective directories.

This script automates the download and extraction of large ZIP files using multiprocessing, enabling efficient and reliable handling of high-volume data. Three parallel processes are responsible for downloading the ZIP files related to company data (`Empresas` and `Estabelecimentos`) and shareholders data (`Socios`), while a fourth process handles the extraction of these files. During extraction, each file is unzipped and renamed according to a standardized naming convention (e.g., `estabelecimentos0.csv`), and placed in the appropriate directory.

> ⚠️ **Note**: This multiprocessing approach greatly improves performance by reducing total processing time, minimizing I/O bottlenecks, and ensuring large datasets are handled efficiently and in an organized manner. However, due to the substantial size of the data, the full download and extraction process may still take a significant amount of time. Patience is recommended—especially on slower internet connections or machines with limited disk throughput.
