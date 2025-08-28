import os
import time
import logging
import threading
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app import process_file  # Импортируй свою функцию обработки

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Папка для наблюдения
WATCH_FOLDER = "/home/test_project/ftp_uploads"
file_queue = Queue()

def wait_until_file_is_ready(filepath, timeout=10, check_interval=1):
    """Ожидает, пока файл не перестанет изменяться (или истечёт таймаут)"""
    last_size = -1
    for _ in range(timeout):
        try:
            current_size = os.path.getsize(filepath)
            if current_size == last_size:
                return True
            last_size = current_size
        except FileNotFoundError:
            pass
        time.sleep(check_interval)
    return False

class UploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        if event.src_path.endswith(".xlsx"):
            filename = os.path.basename(event.src_path)
            logging.info(f"[WATCHER] Найден файл: {filename}, проверяю готовность...")

            if wait_until_file_is_ready(event.src_path):
                logging.info(f"[WATCHER] Файл {filename} готов, добавляю в очередь")
                file_queue.put(event.src_path)
            else:
                logging.error(f"[WATCHER] Файл {filename} не стабилизировался вовремя")

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

    # Запуск потока-обработчика очереди
    threading.Thread(target=worker, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
