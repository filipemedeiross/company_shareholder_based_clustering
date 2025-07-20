## 📁 DATA

This project uses public datasets provided by the [Receita Federal do Brasil (RFB)](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/), which contain detailed information on Brazilian companies and their ownership structures.

### 📂 Data Organization

```
data/
    README.md

    csv/                           # raw data obtained from the RFB
        estabelecimentos/
            estabelecimentos0.csv
            [...]
            estabelecimentos9.csv
        socios/
            socios0.csv
            [...]
            socios9.csv
```

### 📥 Data Ingestion

The datasets must be downloaded and extracted by running the script `scripts/1_data_ingestion.py`. The ingestion process includes:

1. **Downloading** 10 files named `Estabelecimentos[0-9].zip`, which after extraction total approximately **14.3 GB** of company data.
2. **Downloading** 10 files named `Socios[0-9].zip`, which after extraction total approximately **2.47 GB** of partner/shareholder data.
3. **Extracting and renaming** each file to a standardized CSV format and saving them in their respective directories.

This script automates the download and extraction of large ZIP files using multiprocessing, enabling efficient and reliable handling of high-volume data. Two parallel processes are responsible for downloading the ZIP files related to company data (`Estabelecimentos`) and shareholders data (`Socios`), while a third process handles the extraction of these files. During extraction, each file is unzipped and renamed according to a standardized naming convention (e.g., `estabelecimentos0.csv`), and placed in the appropriate directory.

> Note.: This multiprocessing approach significantly reduces total processing time, minimizes resource bottlenecks, and ensures that large datasets are handled in a structured and performant manner.
