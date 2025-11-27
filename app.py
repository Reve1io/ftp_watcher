import os
import pandas as pd
from flask import Flask, request, render_template, flash, redirect, url_for
from flask.cli import load_dotenv
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
import asyncio

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Конфигурация Flask
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx'}

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def upload_to_ssh(file_path):
    if os.name == 'nt':
        logging.info('Windows is not supported, skip upload')
        return True

    try:
        subprocess.run(['/usr/bin/which', 'sshpass'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        logging.warning("⏭️ sshpass not found — skipping SSH upload")
        return True

    ssh_host = os.getenv('STORAGE_IP')
    ssh_port = os.getenv('STORAGE_PORT')
    ssh_user = os.getenv('STORAGE_USER')
    ssh_password = os.getenv('STORAGE_PASSWORD')

    remote_path = f'/home/GetChips_API/project2.0/uploads/{os.path.basename(file_path)}'

    scp_command = (
        f"/usr/bin/sshpass -p {ssh_password} scp -P {ssh_port} "
        f"{file_path} {ssh_user}@{ssh_host}:{remote_path}"
    )

    try:
        subprocess.run(scp_command, shell=True, check=True)
        logging.info(f"Uploaded to SSH: {remote_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"SCP Error: {e.stderr.decode()}")
        raise


def upload_to_ftp(file_path):
    ftp_host = os.getenv('SERVER_HOST')
    ftp_user = os.getenv('SERVER_USER')
    ftp_password = os.getenv('SERVER_PASSWORD')

    logging.info(f"FTP HOST: {ftp_host}, USER: {ftp_user}")

    try:
        with FTP(ftp_host) as ftp:
            ftp.login(ftp_user, ftp_password)
            with open(file_path, 'rb') as f:
                ftp.storbinary(f'STOR {os.path.basename(file_path)}', f)
        logging.info(f"Uploaded to FTP: {file_path}")
    except Exception as e:
        logging.error(f"FTP error: {str(e)}")
        raise


def sanitize_for_1c(obj):
    """
    Рекурсивно заменяет все None на пустые строки для корректной отправки в 1С
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_1c(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_1c(v) for v in obj]
    elif obj is None:
        return ""
    else:
        return obj


def send_octopart_to_1c(data):

    wsdl_url = os.getenv("URL_1C")
    username = os.getenv("USER_1C")
    password = os.getenv("PASSWORD_1C")

    if not wsdl_url or not username or not password:
        logging.error("❌ Не заданы параметры подключения к 1С")
        return

    sanitized_data = sanitize_for_1c(data)
    json_str = json.dumps(sanitized_data, ensure_ascii=False)

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    transport = Transport(session=session)
    settings = Settings(strict=False, xml_huge_tree=True)

    try:
        client = Client(wsdl=wsdl_url, transport=transport, settings=settings)
        response = client.service.ReturnOctopartData(json_str)
        logging.info(f"[1C SOAP] Данные успешно отправлены. Ответ: {response}")
    except Exception as e:
        logging.error(f"[1C SOAP] Ошибка отправки данных в 1С: {str(e)}")


def process_part(part, original_mpn, found_mpn, ALLOWED_SELLERS, requested_quantity=None):
    """
    Универсальная обработка результата одного part
    Возвращает список записей (часто 1+, если несколько цен).
    """

    output_records = []

    # === Безопасное извлечение данных ===
    original_mpn = original_mpn or ""
    part_name = part.get("name") or ""
    manufacturer_node = part.get("manufacturer") or {}
    category_node = part.get("category") or {}
    images = part.get("images") or []
    descriptions = part.get("descriptions") or []
    sellers = part.get("sellers") or []

    # manufacturer
    if isinstance(manufacturer_node, dict):
        manufacturer_id = manufacturer_node.get("id")
        manufacturer_name = manufacturer_node.get("name")
    else:
        manufacturer_id = None
        manufacturer_name = str(manufacturer_node)

    # category
    category_id = category_node.get("id")
    category_name = category_node.get("name")

    # image URL (берём первую)
    image_url = images[0]["url"] if images and isinstance(images[0], dict) else None

    # description (тоже первую)
    description = descriptions[0]["text"] if descriptions and isinstance(descriptions[0], dict) else None


    # === Проходим всех продавцов ===
    for seller in sellers:
        company = seller.get("company") or {}
        seller_name = company.get("name")
        seller_id = company.get("id")
        seller_verified = company.get("isVerified")
        seller_homepageUrl = company.get("homepageUrl")

        if not seller_name:
            continue

        if ALLOWED_SELLERS and seller_name not in ALLOWED_SELLERS:
            continue

        offers = seller.get("offers") or []

        # === Проходим офферы ===
        for offer in offers:
            stock = offer.get("inventoryLevel")
            prices = offer.get("prices") or []

            # цены внутри оффера
            for price in prices:
                base_price = price.get("convertedPrice")
                currency = price.get("convertedCurrency") or price.get("currency")
                offer_quantity = price.get("quantity")

                # защита от кривых данных
                try:
                    base_price = float(base_price)
                except:
                    base_price = None

                # Ценообразование
                delivery_coef = 1.27
                markup = 1.18

                if base_price:
                    target_price_purchasing = base_price * 0.82
                    cost_with_delivery = target_price_purchasing + delivery_coef
                    target_price_sales = target_price_purchasing + delivery_coef + markup
                else:
                    target_price_purchasing = None
                    cost_with_delivery = None
                    target_price_sales = None

                output_records.append({
                    "requested_mpn": original_mpn,
                    "mpn": found_mpn,
                    "manufacturer": manufacturer_name,
                    "manufacturer_id": manufacturer_id,
                    "manufacturer_name": manufacturer_name,

                    "seller_id": seller_id,
                    "seller_name": seller_name,
                    "seller_verified": seller_verified,
                    "seller_homepageUrl": seller_homepageUrl,

                    "stock": stock,
                    "offer_quantity": offer_quantity,
                    "price": base_price,
                    "currency": currency,

                    "category_id": category_id,
                    "category_name": category_name,
                    "image_url": image_url,
                    "description": description,

                    "requested_quantity": requested_quantity,
                    "status": "Найдено",

                    "delivery_coef": delivery_coef,
                    "markup": markup,
                    "target_price_purchasing": round(target_price_purchasing, 2) if target_price_purchasing else None,
                    "cost_with_delivery": round(cost_with_delivery, 2) if cost_with_delivery else None,
                    "target_price_sales": round(target_price_sales, 2) if target_price_sales else None
                })

    return output_records

async def process_all_mpn(mpn_list, mode="xlsx", chunk_size=15, max_retries=3):
    """
    Асинхронная обработка списка MPN.
    1. Получаем все вариации через supSearch.
    2. Получаем детальную информацию через supMultiMatch.
    3. Формируем output_data.
    4. Отправляем в 1С через send_octopart_to_1c.
    """
    clientId = os.getenv("NEXAR_ID")
    clientSecret = os.getenv("NEXAR_TOKEN")
    nexar = NexarClient(clientId, clientSecret)

    ALLOWED_SELLERS = [
        "Mouser", "Digi-Key", "Arrow", "TTI", "ADI",
        "Coilcraft", "Rochester", "Verical", "Texas Instruments", "MINICIRCUITS"
    ]

    output_data = []

    # --- 1. Получение всех вариаций через supSearch ---
    async def partial_request_variations(mpn_item):
        gqlQuery = '''
    query Search ($q: String!) {
      supSearch(q: $q, limit: 50, currency: "USD") {
        results {
          part { 
            mpn
            name
            manufacturer { name }
          }
        }
      }
    }
        '''
        variables = {"q": mpn_item["mpn"]}

        for attempt in range(1, max_retries + 1):
            try:
                result = await asyncio.to_thread(nexar.get_query, gqlQuery, variables) or {}
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                logging.warning(
                    f"Partial-запрос Nexar ошибка ({mpn_item['mpn']}, попытка {attempt}/{max_retries}): {e}. Жду {wait}s."
                )
                await asyncio.sleep(wait)
        else:
            return [mpn_item["mpn"]]

        variants = []
        for item in result.get("supSearch", {}).get("results", []):
            part = item.get("part")
            if part and part.get("mpn"):
                variants.append(part["mpn"])

                #for similar in part.get("similarParts", []):
                    #if similar.get("mpn"):
                        #variants.append(similar["mpn"])

        return variants or [mpn_item["mpn"]]

    # запускаем partial для всех MPN
    partial_tasks = [partial_request_variations(item) for item in mpn_list]
    all_variants_lists = await asyncio.gather(*partial_tasks)

    mapping = {
        item["mpn"]: {
            "variants": variants,
            "quantity": item.get("quantity"),
            "results": {}
        }
        for item, variants in zip(mpn_list, all_variants_lists)
    }

    # --- 2. Получение данных через supMultiMatch ---
    multi_mpn_list = [{"mpn": v} for sublist in all_variants_lists for v in sublist]

    for i in range(0, len(multi_mpn_list), chunk_size):
        chunk = multi_mpn_list[i:i + chunk_size]
        variables = {"queries": [{"mpn": item["mpn"]} for item in chunk]}

        gqlQuery = '''
        query csvDemo($queries: [SupPartMatchQuery!]!) {
          supMultiMatch(currency: "USD", queries: $queries) {
            parts {
              mpn
              name
              category { id name }
              images { url }
              descriptions { text }
              manufacturer { id name }
              sellers {
                company { id name isVerified homepageUrl }
                offers {
                  inventoryLevel
                  prices { quantity currency convertedPrice convertedCurrency }
                }
              }
            }
          }
        }
        '''

        # retry
        for attempt in range(1, max_retries + 1):
            try:
                response = nexar.get_query(gqlQuery, variables) or {}
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                logging.warning(
                    f"Nexar API ошибка (попытка {attempt}/{max_retries}): {e}. Жду {wait}s."
                )
                await asyncio.sleep(wait)
        else:
            logging.error(f"Nexar API не ответил после {max_retries} попыток для чанка {i // chunk_size + 1}")
            continue

        multi_res = response.get("supMultiMatch") or []
        if isinstance(multi_res, dict):
            multi_res = [multi_res]

        for block in multi_res:
            for part in block.get("parts") or []:
                found_mpn = part.get("mpn")
                if not found_mpn:
                    continue
                # распределяем результаты по mapping
                for req_mpn, data in mapping.items():
                    if found_mpn in data["variants"]:
                        data["results"][found_mpn] = part
                        break

    flat_output = []

    for requested_mpn, data in mapping.items():
        qty = data.get("quantity")
        results = data.get("results") or {}

        if not results:
            flat_output.append({
                "requested_mpn": requested_mpn,
                "status": "Не найдено"
            })
            continue

        for found_mpn, part in results.items():
            rows = process_part(
                part=part,
                original_mpn=requested_mpn,
                found_mpn=found_mpn,
                ALLOWED_SELLERS=ALLOWED_SELLERS,
                requested_quantity=qty
            )
            flat_output.extend(rows)

    return flat_output

async def process_file_async(filepath):
    df = pd.read_excel(filepath, header=None, engine='openpyxl')

    mpn_list = []
    for _, row in df.iterrows():
        mpn_list.append({
            "mpn": str(row[0]).strip(),
            "quantity": int(row[1]) if len(row) > 1 else 1
        })

    flat_json = await process_all_mpn(mpn_list)

    send_octopart_to_1c(flat_json)

    return flat_json

def process_file(filepath):
    return asyncio.run(process_file_async(filepath))

@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":

        if 'file' not in request.files:
            flash("Файл не найден в запросе")
            return redirect(request.url)

        file = request.files['file']
        if file.filename == '':
            flash("Файл не выбран")
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            saved_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(saved_path)

            try:
                output_path = process_file(saved_path)
                flash(f"Готово! Файл выгружен: {os.path.basename(output_path)}")
            except Exception as e:
                flash(f"Ошибка: {str(e)}")

            return redirect(url_for("upload_file"))

    return render_template("index.html")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5004)), debug=True)
