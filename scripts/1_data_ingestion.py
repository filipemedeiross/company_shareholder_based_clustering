import shutil
import zipfile
import requests

from pathlib import Path
from multiprocessing import Process,      \
                            JoinableQueue


BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/"

ROOT_DIR = Path(__file__).resolve().parent.parent
TMP_DIR  = ROOT_DIR / "tmp"
DICT_DIR = {
    'Estabelecimentos' : ROOT_DIR / "data/csv/estabelecimentos",
    'Empresas'         : ROOT_DIR / "data/csv/empresas"        ,
    'Socios'           : ROOT_DIR / "data/csv/socios"          ,
}


def ensure_dirs():
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    for path in DICT_DIR.values():
        path.mkdir(parents=True, exist_ok=True)


def download_file(url, dest):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=524288):
                f.write(chunk)


def extract_and_rename(zip_path, target_dir, prefix):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        [file] = zip_ref.namelist()
        out_path = target_dir / f"{prefix}.csv"

        with zip_ref.open(file) as src, open(out_path, 'wb') as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)


def downloader_worker(file_type, total, task_queue):
    target_dir = DICT_DIR[file_type]

    for i in range(total):
        zip_exists = target_dir / f"{file_type.lower()}{i}.csv"

        if zip_exists.exists():
            print(f"[Download] Already exists: {zip_exists.name}, skipping download.")
        else:
            url      = f"{BASE_URL}{file_type}{i}.zip"
            zip_path = TMP_DIR / f"{file_type}{i}.zip"

            print(f"[Download] Downloading {url}...")
            try:
                download_file(url, zip_path)
                task_queue.put((zip_path, target_dir, f"{file_type.lower()}{i}"))
            except Exception as e:
                print(f"[Download Error] {url}: {e}")


def extractor_worker(task_queue):
    while True:
        try:
            item = task_queue.get()

            if item is None:
                task_queue.task_done()
                break

            zip_path, target_dir, prefix = item

            print(f"[Extraction] Extracting {zip_path}...")
            try:
                extract_and_rename(zip_path, target_dir, prefix)
            except Exception as e:
                print(f"[Extraction Error] {zip_path}: {e}")
            finally:
                task_queue.task_done()
        except Exception as e:
            print(f"[General Extraction Error]: {e}")


def main():
    ensure_dirs()

    task_queue = JoinableQueue()

    downloaders = [
        Process(target=downloader_worker, args=("Estabelecimentos", 10, task_queue)),
        Process(target=downloader_worker, args=("Empresas"        , 10, task_queue)),
        Process(target=downloader_worker, args=("Socios"          , 10, task_queue)),
    ]
    extractor = Process(target=extractor_worker, args=(task_queue,))

    for downloader in downloaders:
        downloader.start()
    extractor.start()

    for downloader in downloaders:
        downloader.join()

    task_queue.put(None)
    task_queue.join()
    extractor.join()

    print("Removing temporary files...")
    shutil.rmtree(TMP_DIR)


if __name__ == "__main__":
    main()
