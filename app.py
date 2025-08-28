import os
import pandas as pd
from flask import Flask, request, render_template, flash, redirect, url_for
from werkzeug.utils import secure_filename
import subprocess
from ftplib import FTP
from nexarClient import NexarClient
import logging
import json
from zeep import Client, Settings
from zeep.transports import Transport
import requests
from requests.auth import HTTPBasicAuth

# Настройка логирования+
logging.basicConfig(level=logging.DEBUG)

# Конфигурация Flask
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx'}

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
# �send getchips file from  HTTP web FLASK
def upload_to_ssh(file_path):
    ssh_host = '83.69.192.170'
    ssh_port = '5034'
    ssh_user = 'root'
    ssh_password = 'B6z2S9gwn29J'
    remote_path = f'/home/GetChips_API/project2.0/uploads/{os.path.basename(file_path)}'

    scp_command = f"sshpass -p {ssh_password} scp -P {ssh_port} {file_path} {ssh_user}@{ssh_host}:{remote_path}"

    try:
        logging.info(f"Executing command: {scp_command}")
        subprocess.run(scp_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"File {file_path} successfully uploaded to {remote_path}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during SCP: {e.stderr.decode('utf-8')}")
        raise
# fullfile after octopart 
def upload_to_ftp(file_path):
    ftp_host = 'nmarchj5.beget.tech'
    ftp_user = 'nmarchj5_nexar'
    ftp_password = 'Yk0P28M!ZgHW'
    try:
        with FTP(ftp_host) as ftp:
            ftp.login(ftp_user, ftp_password)
            with open(file_path, 'rb') as file:
                ftp.storbinary(f'STOR {os.path.basename(file_path)}', file)
            logging.info(f'Файл {file_path} успешно загружен на FTP.')
    except Exception as e:
        logging.error(f"Ошибка при загрузке на FTP: {str(e)}")
        raise
# connect to1c
def send_octopart_to_1c(data: list):
    wsdl_url = "http://web1c.radiant.local/erp_base/ws/ExchangeXML.1cws?wsdl"
    username = "ExchangeOctopart"
    password = "12345"

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    transport = Transport(session=session)
    settings = Settings(strict=False, xml_huge_tree=True)

    client = Client(wsdl=wsdl_url, transport=transport, settings=settings)

    json_payload = json.dumps(data, ensure_ascii=False)

    try:
        response = client.service.ReturnOctopartData(json_payload)
        logging.info(f"[1C SOAP] Ответ от 1С: {response}")
    except Exception as e:
        logging.error(f"[1C SOAP] Ошибка отправки в 1С: {e}")
# excel obrabotka
def process_chunk(mpns):
    gqlQuery = '''
    query csvDemo ($queries: [SupPartMatchQuery!]!) {
      supMultiMatch (
        currency: "EUR",
        queries: $queries
      ) {
        parts {
          mpn
          name
          sellers {
            company {
              id
              name
            }
            offers {
              inventoryLevel
              prices {
                quantity
                convertedPrice
                convertedCurrency
              }
            }
          }
        }
      }
    }
    '''

    clientId = os.environ.get("NEXAR_CLIENT_ID")
    clientSecret = os.environ.get("NEXAR_CLIENT_SECRET")
    nexar = NexarClient(clientId, clientSecret)

    queries = [{"mpn": str(mpn)} for mpn in mpns]
    variables = {"queries": queries}

    try:
        results = nexar.get_query(gqlQuery, variables)
    except Exception as e:
        logging.error(f"Ошибка при GraphQL-запросе: {str(e)}")
        raise

    output_data = []
    for query, mpn in zip(results.get("supMultiMatch", []), mpns):
        parts = query.get("parts", [])
        for part in parts:
            part_name = part.get("name", "")
            part_manufacturer = part_name.rsplit(' ', 1)[0]
            sellers = part.get("sellers", [])
            for seller in sellers:
                seller_name = seller.get("company", {}).get("name", "")
                seller_id = seller.get("company", {}).get("id", "")
                offers = seller.get("offers", [])
                for offer in offers:
                    stock = offer.get("inventoryLevel", "")
                    prices = offer.get("prices", [])
                    for price in prices:
                        quantity = price.get("quantity", "")
                        converted_price = price.get("convertedPrice", "")
                        output_data.append([mpn, part_manufacturer, seller_id, seller_name, stock, quantity, converted_price])
    return output_data
# octopart general function 
def process_file(input_excel_path):
    try:
        if not os.path.exists(input_excel_path):
            raise FileNotFoundError(f"Файл {input_excel_path} не найден.")

        upload_to_ssh(input_excel_path)

        df = pd.read_excel(input_excel_path)
        if df.empty:
            raise ValueError("Входной файл пуст.")

        mpns = df.iloc[:, 0].dropna().tolist()
        chunk_size = 50
        all_output_data = []

        for i in range(0, len(mpns), chunk_size):
            chunk_mpns = mpns[i:i + chunk_size]
            try:
                chunk_output_data = process_chunk(chunk_mpns)
                all_output_data.extend(chunk_output_data)
            except Exception as e:
                logging.error(f"Ошибка обработки блока {i // chunk_size + 1}: {str(e)}")

        if not all_output_data:
            raise ValueError("Не удалось получить данные ни по одной позиции.")

        output_df = pd.DataFrame(
            all_output_data,
            columns=["MPN", "Название", "ID продавца", "Имя продавца", "Запас", "Количество", "Цена (EUR)"]
        )

        original_name = os.path.splitext(os.path.basename(input_excel_path))[0]
        output_filename = f"{original_name}_response.xlsx"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)

        output_df.to_excel(output_path, index=False, encoding='utf-8')
        upload_to_ftp(output_path)

        logging.info(f"Общий файл сохранён: {output_path}")

        # Отправка в 1С
        final_payload = [
            {
                "MPN": row[0],
                "Manufacturer": row[1],
                "SellerID": row[2],
                "SellerName": row[3],
                "Stock": row[4],
                "Quantity": row[5],
                "Price": row[6],
                "Currency": "EUR"
            } for row in all_output_data
        ]

        send_octopart_to_1c(final_payload)

        return [output_path]

    except Exception as e:
        logging.error(f"Ошибка при обработке файла: {str(e)}")
        raise

@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        if 'file' not in request.files:
            flash('Файл не найден в запросе')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('Файл не выбран')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            input_excel_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(input_excel_path)
            try:
                output_files = process_file(input_excel_path)
                if isinstance(output_files, list):
                    flash(f'Успешно выгружены файлы: {", ".join(os.path.basename(f) for f in output_files)}')
                else:
                    flash(f'Файл {output_files} успешно выгружен.')
            except Exception as e:
                flash(f'Ошибка при обработке: {str(e)}')
            return redirect(url_for('upload_file'))
    return render_template("index.html")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
