import shutil
from multiprocessing import Process,      \
                            JoinableQueue

from .io import downloader_worker, \
                extractor_worker , \
                ensure_dirs
from .constants import BASE_URL, \
                       TMP_DIR , \
                       DICT_DIR


def main():
    ensure_dirs([TMP_DIR, *DICT_DIR.values()])

    task_queue = JoinableQueue()

    downloaders = [
        Process(
            target=downloader_worker,
            args=(
                BASE_URL  ,
                file_type ,
                10        ,
                target_dir,
                TMP_DIR   ,
                task_queue,
            )
        )
        for file_type, target_dir in DICT_DIR.items()
    ]
    extractor = Process(target=extractor_worker, args=(task_queue,))

    for downloader in downloaders:
        downloader.start()
    extractor.start()

    for downloader in downloaders:
        downloader.join()

    task_queue.put(None)
    task_queue.join()
    extractor .join()

    print("Removing temporary files...")
    shutil.rmtree(TMP_DIR)


if __name__ == "__main__":
    main()
