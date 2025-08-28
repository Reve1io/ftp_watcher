import os
import time
import logging
import threading
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app import process_file  # ваша функция

logging.basicConfig(level=logging.INFO)
WATCH_FOLDER = "/home/test_project/ftp_uploads"
file_queue = Queue()

SUPPORTED = (".xlsx", ".csv")

def wait_until_file_is_ready(filepath, timeout=30, check_interval=1):
    """Ждём стабилизации размера/mtime, чтобы не ловить недописанные файлы."""
    last_size = -1
    last_mtime = -1
    stable_ticks = 0
    needed_stable = 2  # два подряд стабильных замера

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st = os.stat(filepath)
            if st.st_size == last_size and st.st_mtime == last_mtime:
                stable_ticks += 1
                if stable_ticks >= needed_stable:
                    return True
            else:
                stable_ticks = 0
                last_size, last_mtime = st.st_size, st.st_mtime
        except FileNotFoundError:
            pass
        time.sleep(check_interval)
    return False

class UploadHandler(FileSystemEventHandler):
    def _maybe_enqueue(self, path):
        if not os.path.isfile(path):
            return
        if not path.lower().endswith(SUPPORTED):
            return
        filename = os.path.basename(path)
        logging.info(f"[WATCHER] Найден файл: {filename}, проверяю готовность...")
        if wait_until_file_is_ready(path):
            logging.info(f"[WATCHER] Файл {filename} готов, добавляю в очередь")
            file_queue.put(path)
        else:
            logging.error(f"[WATCHER] Файл {filename} не стабилизировался вовремя")

    def on_created(self, event):
        if not event.is_directory:
            self._maybe_enqueue(event.src_path)

    # иногда клиенты создают через tmp → потом переименовывают
    def on_moved(self, event):
        if not event.is_directory:
            self._maybe_enqueue(event.dest_path)

def worker():
    while True:
        filepath = file_queue.get()
        if filepath is None:
            break
        try:
            logging.info(f"[QUEUE] Обработка файла: {filepath}")
            process_file(filepath)
        except Exception as e:
            logging.error(f"[QUEUE] Ошибка при обработке файла {filepath}: {e}")
        finally:
            file_queue.task_done()

if __name__ == "__main__":
    logging.info(f"[WATCHER] Наблюдение за папкой {WATCH_FOLDER} начато...")

    observer = Observer()
    observer.schedule(UploadHandler(), path=WATCH_FOLDER, recursive=False)
    observer.start()

    threading.Thread(target=worker, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()