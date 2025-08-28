# gunicorn_config.py

import multiprocessing

bind = "0.0.0.0:8000"  # Указываем, что приложение будет доступно на всех интерфейсах на порту 8000
workers = multiprocessing.cpu_count() * 2 + 1  # Оптимальное количество воркеров
