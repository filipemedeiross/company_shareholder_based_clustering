import shutil
import zipfile
import requests


def ensure_dirs(paths):
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def download_file(url, dest, chunk_size=524288):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size):
                f.write(chunk)


def extract_and_rename(zip_path, filename, length=1024*1024):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        [file] = zip_ref.namelist()

        with zip_ref.open(file) as src, open(filename, 'wb') as dst:
            shutil.copyfileobj(src, dst, length)


def downloader_worker(url, file_type, total, target_dir, tmp_dir, task_queue):
    for i in range(total):
        filename = target_dir / f"{file_type.lower()}{i}.csv"

        if filename.exists():
            print(f"[Download] Already exists: {filename.name}, skipping download.")
        else:
            url_path = f"{url}{file_type}{i}.zip"
            zip_path = tmp_dir / f"{file_type}{i}.zip"

            print(f"[Download] Downloading {url_path}...")

            try:
                download_file(url_path, zip_path)
                task_queue.put((zip_path, filename))
            except Exception as e:
                print(f"[Download Error] {url_path}: {e}")


def extractor_worker(task_queue):
    while True:
        try:
            item = task_queue.get()
            if item is None:
                task_queue.task_done()
                break

            zip_path, filename = item

            print(f"[Extraction] Extracting {zip_path.name}...")
            try:
                extract_and_rename(zip_path, filename)
            except Exception as e:
                print(f"[Extraction Error] {zip_path}: {e}")
            finally:
                task_queue.task_done()

        except Exception as e:
            print(f"[General Extraction Error]: {e}")
