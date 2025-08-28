import pandas as pd
import requests
from ftplib import FTP
import os

# Новый токен
token = '0f106028b4d46757547f67a856dcd1e8'

def clean_data(df):
    """Функция для очистки данных."""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.encode('utf-8', errors='ignore').str.decode('utf-8')
    return df

def process_other_file(input_excel_path):
    """Функция для обработки входного Excel файла."""
    try:
        print(f"Обработка файла: {input_excel_path}")
        
        df = pd.read_excel(input_excel_path)
        print(f"Данные успешно прочитаны. Формат: {df.shape[0]} строк и {df.shape[1]} столбцов.")

        # Очистка данных перед обработкой
        df = clean_data(df)

        # Проверяем, что в Excel есть колонка 'mpn'
        if 'mpn' not in df.columns:
            print("В Excel файле нет колонки 'mpn'.")
            return

        results = []

        for index, row in df.iterrows():
            # Удаление лишних пробелов и символов новой строки
            mpn = row['mpn'].strip()  # Убираем пробелы и символы новой строки
            qty = 1  # Укажите нужное количество

            url = f'https://api.client-service.getchips.ru/client/api/gh/v1/search/partnumber?input={mpn}&qty={qty}&token={token}'
            headers = {'Accept': 'application/json'}
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Проверка на успешный статус-код

                data = response.json()
                for item in data.get('data', []):
                    results.append({
                        'mpn': mpn,
                        'title': item.get('title'),
                        'donorID': item.get('donorID'),
                        'donor': item.get('donor'),
                        'quantity': item.get('quantity'),
                        'eQuantity': item.get('eQuantity'),
                        'price': item.get('price')
                    })

            except requests.HTTPError as http_err:
                print(f"Ошибка HTTP для MPN {mpn}: {http_err}")
                results.append({'mpn': mpn, 'error': response.status_code})
            except Exception as e:
                print(f"Ошибка при запросе для MPN {mpn}: {e}")
                results.append({'mpn': mpn, 'error': str(e)})

        results_df = pd.DataFrame(results)
        
        output_file = 'uploads/GetChips_response.xlsx'
        results_df.to_excel(output_file, index=False)

        # Выгрузка файла на FTP-сервер
        upload_to_ftp(output_file)

        print(f'Файл {output_file} успешно сохранен и загружен на FTP-сервер.')
        return output_file  # Возвращаем путь к выходному файлу

    except Exception as e:
        print(f'Ошибка при обработке файла: {str(e)}')

def upload_to_ftp(file_path):
    """Функция для загрузки файла на FTP-сервер."""
    ftp_host = 'ftp.nmarchj5.beget.tech'
    ftp_user = 'nmarchj5_getchips_upload'
    ftp_password = '7DK4Rr!sXoxR'
    
    try:
        with FTP(ftp_host) as ftp:
            ftp.login(ftp_user, ftp_password)
            with open(file_path, 'rb') as file:
                ftp.storbinary(f'STOR {os.path.basename(file_path)}', file)
        
        print(f'Файл {file_path} успешно загружен на FTP-сервер.')

    except Exception as e:
        print(f'Ошибка при загрузке файла на FTP: {str(e)}')

if __name__ == '__main__':
    process_other_file('uploads/input.xlsx')  # Пример для локального запуска
