import os
import json
import logging
import zipfile
import tempfile
import shutil
import subprocess
from ftplib import FTP

import pandas as pd
import requests
from flask import Flask, request, render_template, flash, redirect, url_for
from werkzeug.utils import secure_filename
from requests.auth import HTTPBasicAuth
from zeep import Client, Settings
from zeep.transports import Transport

from nexarClient import NexarClient


# -------------------- ЛОГИ --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
log = logging.getLogger("app")


# -------------------- КОНФИГ --------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
ALLOWED_EXTENSIONS = {"xlsx", "csv"}

SSH_HOST = "83.69.192.170"
SSH_PORT = "5034"
SSH_USER = "root"
SSH_PASS = "B6z2S9gwn29J"

FTP_HOST = "nmarchj5.beget.tech"
FTP_USER = "nmarchj5_nexar"
FTP_PASS = "Yk0P28M!ZgHW"

WSDL_URL = "http://web1c.radiant.local/erp_base/ws/ExchangeXML.1cws?wsdl"
SOAP_USER = "ExchangeOctopart"
SOAP_PASS = "12345"


# -------------------- FLASK --------------------
app = Flask(__name__)
app.secret_key = "your_secret_key"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# -------------------- УТИЛИТЫ --------------------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _fix_xlsx_via_libreoffice(src_path: str) -> str:
    """Конвертирует «битый» XLSX в корректный XLSX через LibreOffice (headless).
       Возвращает путь к исправленному файлу во временной папке.
    """
    outdir = tempfile.mkdtemp(prefix="xlsxfix_")
    cmd = ["soffice", "--headless", "--convert-to", "xlsx", "--outdir", outdir, src_path]
    log.warning("[XLSX-FIX] Конвертирую через LibreOffice: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # LibreOffice может назвать файл по-разному — найдём любой .xlsx в outdir
    for name in os.listdir(outdir):
        if name.lower().endswith(".xlsx"):
            return os.path.join(outdir, name)
    raise RuntimeError("LibreOffice не создал файл .xlsx")


def read_excel_robust(path: str) -> pd.DataFrame:
    """Чтение XLSX. Если отсутствует xl/sharedStrings.xml (часто у 1С) — чиним."""
    try:
        return pd.read_excel(path, engine="openpyxl")
    except Exception as e1:
        log.warning("[XLSX] Обычное чтение не удалось: %s", e1)

    needs_fix = True
    try:
        with zipfile.ZipFile(path) as zf:
            needs_fix = ("xl/sharedStrings.xml" not in zf.namelist())
    except zipfile.BadZipFile:
        needs_fix = True

    if needs_fix:
        fixed = _fix_xlsx_via_libreoffice(path)
        try:
            df = pd.read_excel(fixed, engine="openpyxl")
            shutil.rmtree(os.path.dirname(fixed), ignore_errors=True)
            return df
        except Exception as e2:
            log.error("[XLSX-FIX] Чтение после конвертации не удалось: %s", e2)
            raise

    raise RuntimeError("Не удалось прочитать XLSX (и фикса не потребовалось?)")


def read_csv_robust(path: str) -> pd.DataFrame:
    # sep=None включает автоопределение разделителя; utf-8-sig съедает BOM
    return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")


def read_table_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsx":
        return read_excel_robust(path)
    if ext == ".csv":
        return read_csv_robust(path)
    raise ValueError(f"Неподдержимое расширение: {ext}")


# -------------------- ПЕРЕКАЧКА ФАЙЛОВ --------------------
def upload_to_ssh(file_path: str) -> None:
    remote_path = f"/home/GetChips_API/project2.0/uploads/{os.path.basename(file_path)}"
    scp_command = (
        f"sshpass -p '{SSH_PASS}' scp -P {SSH_PORT} "
        f"'{file_path}' {SSH_USER}@{SSH_HOST}:'{remote_path}'"
    )
    try:
        log.info("SCP upload → %s:%s (команда без пароля не логируем)", SSH_HOST, remote_path)
        subprocess.run(scp_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        log.info("Файл %s загружен на %s.", file_path, remote_path)
    except subprocess.CalledProcessError as e:
        log.error("Ошибка SCP: %s", e.stderr.decode("utf-8", errors="ignore"))
        raise


def upload_to_ftp(file_path: str) -> None:
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            with open(file_path, "rb") as f:
                ftp.storbinary(f"STOR {os.path.basename(file_path)}", f)
        log.info("Файл %s успешно загружен на FTP.", file_path)
    except Exception as e:
        log.error("Ошибка при загрузке на FTP: %s", e)
        raise


# -------------------- SOAP В 1С --------------------
def send_octopart_to_1c(data: list) -> None:
    session = requests.Session()
    session.auth = HTTPBasicAuth(SOAP_USER, SOAP_PASS)
    transport = Transport(session=session)
    settings = Settings(strict=False, xml_huge_tree=True)

    client = Client(wsdl=WSDL_URL, transport=transport, settings=settings)
    json_payload = json.dumps(data, ensure_ascii=False)

    try:
        response = client.service.ReturnOctopartData(json_payload)
        log.info("[1C SOAP] Ответ от 1С: %s", response)
    except Exception as e:
        log.error("[1C SOAP] Ошибка отправки в 1С: %s", e)


# -------------------- OCTOPART --------------------
def process_chunk(mpns):
    gqlQuery = '''
    query csvDemo ($queries: [SupPartMatchQuery!]!) {
      supMultiMatch (currency: "EUR", queries: $queries) {
        parts {
          mpn
          name
          sellers {
            company { id name }
            offers {
              inventoryLevel
              prices { quantity convertedPrice convertedCurrency }
            }
          }
        }
      }
    }
    '''

    clientId = os.environ.get("NEXAR_CLIENT_ID")
    clientSecret = os.environ.get("NEXAR_CLIENT_SECRET")
    nexar = NexarClient(clientId, clientSecret)

    queries = [{"mpn": str(m)} for m in mpns]
    variables = {"queries": queries}

    results = nexar.get_query(gqlQuery, variables)

    output_data = []
    for query, mpn in zip(results.get("supMultiMatch", []), mpns):
        for part in query.get("parts", []):
            part_name = part.get("name", "") or ""
            part_manufacturer = part_name.rsplit(" ", 1)[0]
            for seller in part.get("sellers", []):
                seller_name = seller.get("company", {}).get("name", "")
                seller_id = seller.get("company", {}).get("id", "")
                for offer in seller.get("offers", []):
                    stock = offer.get("inventoryLevel", "")
                    for price in offer.get("prices", []):
                        quantity = price.get("quantity", "")
                        converted_price = price.get("convertedPrice", "")
                        output_data.append([
                            mpn, part_manufacturer, seller_id, seller_name, stock, quantity, converted_price
                        ])
    return output_data


# -------------------- ОБЩАЯ ОБРАБОТКА --------------------
def process_file(input_path: str):
    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Файл {input_path} не найден.")

        # Сначала зальём исходник на удалённый сервер (как и раньше)
        upload_to_ssh(input_path)

        # Прочитаем таблицу (XLSX/CSV), возьмём первую колонку как MPN
        df = read_table_any(input_path)
        if df.shape[1] < 1:
            raise ValueError("Входной файл без колонок.")
        if df.empty:
            raise ValueError("Входной файл пуст.")

        mpns = (
            df.iloc[:, 0]
            .dropna()
            .astype(str)
            .map(lambda s: s.strip())
            .tolist()
        )
        # Сохраняем порядок, убираем дубли
        mpns = list(dict.fromkeys([m for m in mpns if m]))

        if not mpns:
            raise ValueError("Не нашёл ни одного MPN в первой колонке.")

        chunk_size = 50
        all_output_data = []
        for i in range(0, len(mpns), chunk_size):
            chunk_mpns = mpns[i:i + chunk_size]
            try:
                all_output_data.extend(process_chunk(chunk_mpns))
            except Exception as e:
                log.error("Ошибка обработки блока %s: %s", i // chunk_size + 1, e)

        if not all_output_data:
            raise ValueError("Не удалось получить данные ни по одной позиции.")

        output_df = pd.DataFrame(
            all_output_data,
            columns=["MPN", "Название", "ID продавца", "Имя продавца", "Запас", "Количество", "Цена (EUR)"]
        )

        original_name = os.path.splitext(os.path.basename(input_path))[0]
        output_filename = f"{original_name}_response.xlsx"
        output_path = os.path.join(app.config["UPLOAD_FOLDER"], output_filename)

        # Пишем в XLSX (openpyxl), без ненужного encoding
        output_df.to_excel(output_path, index=False, engine="openpyxl")
        upload_to_ftp(output_path)

        log.info("Общий файл сохранён: %s", output_path)

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
                "Currency": "EUR",
            } for row in all_output_data
        ]
        send_octopart_to_1c(final_payload)

        return [output_path]

    except Exception as e:
        log.error("Ошибка при обработке файла: %s", e)
        raise


# -------------------- ВЕБ --------------------
@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        if "file" not in request.files:
            flash("Файл не найден в запросе")
            return redirect(request.url)

        file = request.files["file"]
        if file.filename == "":
            flash("Файл не выбран")
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            input_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(input_path)
            try:
                output_files = process_file(input_path)
                if isinstance(output_files, list):
                    flash("Успешно выгружены файлы: " + ", ".join(os.path.basename(f) for f in output_files))
                else:
                    flash(f"Файл {output_files} успешно выгружен.")
            except Exception as e:
                flash(f"Ошибка при обработке: {str(e)}")
            return redirect(url_for("upload_file"))

        flash("Неподдерживаемый тип файла")
        return redirect(request.url)

    return render_template("index.html")


if __name__ == "__main__":
    # При запуске через systemd рабочая директория может отличаться;
    # гарантируем существование каталога загрузок.
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)