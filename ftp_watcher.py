import os
import time
import logging
import threading
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app import process_file  # Импортируй свою функцию обработки

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        #logging.FileHandler('/var/log/file-watcher.log'),
        logging.StreamHandler()
    ]
)

# Папка для наблюдения
WATCH_FOLDER = ""
file_queue = Queue()

if os.name == "nt":
    WATCH_FOLDER = "D:/dev/ftp_watcher/watch"
else:
    WATCH_FOLDER = "/home/test_project/ftp_uploads"

def wait_until_file_is_ready(filepath, timeout=60, check_interval=5):
    """Ожидает, пока файл не перестанет изменяться"""
    last_size = -1
    stable_count = 0
    required_stable_checks = 5  # Требуем 3 стабильных проверки подряд
    
    for attempt in range(timeout):
        try:
            if not os.path.exists(filepath):
                logging.warning(f"Файл {filepath} не найден, ожидание...")
                time.sleep(check_interval)
                continue
                
            current_size = os.path.getsize(filepath)
            if current_size == last_size:
                stable_count += 1
                if stable_count >= required_stable_checks:
                    logging.info(f"Файл стабилизирован после {attempt} секунд")
                    return True
            else:
                stable_count = 0
                last_size = current_size
                
        except Exception as e:
            logging.warning(f"Ошибка проверки файла {filepath}: {e}")
            
        time.sleep(check_interval)
    
    logging.warning(f"Файл {filepath} не стабилизировался за {timeout} секунд")
    return False

class UploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        if event.src_path.endswith((".xlsx", ".xls")):
            filename = os.path.basename(event.src_path)
            logging.info(f"📁 Обнаружен новый файл: {filename}")
            
            # Даем файлу время на полную загрузку
            time.sleep(2)
            
            if wait_until_file_is_ready(event.src_path):
                logging.info(f"✅ Файл {filename} готов к обработке")
                file_queue.put(os.path.normpath(event.src_path))
            else:
                logging.error(f"❌ Файл {filename} не готов к обработке")

def worker():
    """Рабочий поток для обработки файлов"""
    logging.info("👷 Worker thread started")
    while True:
        filepath = file_queue.get()
        if filepath is None:
            break
            
        try:
            if os.path.exists(filepath):
                logging.info(f"🔄 Начало обработки: {os.path.basename(filepath)}")
                process_file(filepath)
                logging.info(f"✅ Успешно обработан: {os.path.basename(filepath)}")
            else:
                logging.error(f"❌ Файл не найден: {filepath}")
                
        except Exception as e:
            logging.error(f"💥 Ошибка обработки {filepath}: {e}")
        finally:
            file_queue.task_done()

def main():
    """Основная функция запуска watcher"""
    # Создаем папку для наблюдения если её нет
    os.makedirs(WATCH_FOLDER, exist_ok=True)
    
    logging.info(f"🚀 Запуск File Watcher для папки: {WATCH_FOLDER}")
    logging.info(f"📊 Размер очереди: {file_queue.qsize()}")
    
    # Запускаем рабочий поток
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()
    
    # Настраиваем наблюдатель
    observer = Observer()
    event_handler = UploadHandler()
    observer.schedule(event_handler, WATCH_FOLDER, recursive=False)
    
    try:
        observer.start()
        logging.info("👀 Наблюдатель запущен и работает...")
        
        # Бесконечный цикл для поддержания работы
        while True:
            time.sleep(60)  # Проверяем каждую минуту
            # Можно добавить периодические проверки состояния здесь
            
    except KeyboardInterrupt:
        logging.info("🛑 Получен сигнал остановки...")
    except Exception as e:
        logging.error(f"💥 Критическая ошибка: {e}")
    finally:
        logging.info("🧹 Завершение работы...")
        observer.stop()
        observer.join()
        file_queue.put(None)  # Сигнал остановки worker'у
        worker_thread.join(timeout=10)

if __name__ == "__main__":
    main()
