import shutil
import zipfile
import requests

from pathlib import Path
from multiprocessing import Process,      \
                            JoinableQueue


BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/"

ROOT_DIR = Path(__file__).resolve().parent.parent
TMP_DIR              = ROOT_DIR / "tmp"
ESTABELECIMENTOS_DIR = ROOT_DIR / "data/csv/estabelecimentos"
SOCIOS_DIR           = ROOT_DIR / "data/csv/socios"


def ensure_dirs():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    ESTABELECIMENTOS_DIR.mkdir(parents=True, exist_ok=True)
    SOCIOS_DIR.mkdir(parents=True, exist_ok=True)


def download_file(url, dest):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=131072):
                f.write(chunk)


def extract_and_rename(zip_path, target_dir, prefix):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        [file] = zip_ref.namelist()
        out_path = target_dir / f"{prefix}.csv"

        with zip_ref.open(file) as src, open(out_path, 'wb') as dst:
            shutil.copyfileobj(src, dst)


def downloader_worker(file_type, total, task_queue):
    target_dir = ESTABELECIMENTOS_DIR               \
                 if file_type == "Estabelecimentos" \
                 else SOCIOS_DIR

    for i in range(total):
        url      = f"{BASE_URL}{file_type}{i}.zip"
        zip_path = TMP_DIR / f"{file_type}{i}.zip"

        print(f"[Download] Baixando {url}...")
        try:
            download_file(url, zip_path)
            task_queue.put((zip_path, target_dir, f"{file_type.lower()}{i}"))
        except Exception as e:
            print(f"[Erro no download] {url}: {e}")


def extractor_worker(task_queue):
    while True:
        try:
            item = task_queue.get()

            if item is None:
                task_queue.task_done()
                break

            zip_path, target_dir, prefix = item

            print(f"[Extração] Extraindo {zip_path}...")
            try:
                extract_and_rename(zip_path, target_dir, prefix)
            except Exception as e:
                print(f"[Erro na extração] {zip_path}: {e}")
            finally:
                task_queue.task_done()
        except Exception as e:
            print(f"[Erro geral na extração]: {e}")


def main():
    ensure_dirs()

    task_queue = JoinableQueue()

    downloaders = [
        Process(target=downloader_worker, args=("Estabelecimentos", 10, task_queue)),
        Process(target=downloader_worker, args=("Socios"          , 10, task_queue)),
    ]
    extractor   = Process(target=extractor_worker, args=(task_queue,))

    for downloader in downloaders:
        downloader.start()
    extractor.start()

    for downloader in downloaders:
        downloader.join()

    task_queue.put(None)
    task_queue.join()
    extractor.join()

    print("Removendo arquivos temporários...")
    shutil.rmtree(TMP_DIR)


if __name__ == "__main__":
    main()
