import requests
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time

conn = sqlite3.connect('mse_stock_data.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        issuer_code TEXT,
        date TEXT,
        last_trade_price REAL,
        max_price REAL,
        min_price REAL,
        avg_price REAL,
        percentage_change REAL,
        volume INTEGER,
        turnover_bes  AL,
        total_turnover REAL,
        PRIMARY KEY (issuer_code, date)
    )
''')
conn.commit()


def get_issuer_codes():
    url = "https://www.mse.mk/en/stats/symbolhistory/TEL"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    retry_count = 0
    while retry_count < 5:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")
            dropdown = soup.find("select", {"id": "Code"})
            issuer_codes = [option.get("value") for option in dropdown.find_all("option") if
                            option.get("value") and not any(char.isdigit() for char in option.get("value"))]
            return issuer_codes
        elif response.status_code == 503:
            print("Service unavailable (503). Retrying...")
            time.sleep(5)
            retry_count += 1
        else:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            return []

    print("Failed after 5 retries. Exiting.")
    return []


def get_stock_data(symbol, start_date, end_date):
    url = f"https://www.mse.mk/en/stats/symbolhistory/{symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    form_data = {
        "FromDate": start_date.strftime('%Y-%m-%d'),
        "ToDate": end_date.strftime('%Y-%m-%d'),
        "Code": symbol
    }

    retry_count = 0
    while retry_count < 5:
        response = requests.get(url, headers=headers, params=form_data)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")
            data = []
            table = soup.find("table", {"id": "resultsTable"})
            if table:
                for row in table.find_all("tr")[1:]:
                    cols = row.find_all("td")
                    data.append({
                        "date": cols[0].text.strip(),
                        "last_trade_price": float(cols[1].text.replace(',', '')) if cols[1].text.strip() else 0.0,
                        "max_price": float(cols[2].text.replace(',', '')) if cols[2].text.strip() else 0.0,
                        "min_price": float(cols[3].text.replace(',', '')) if cols[3].text.strip() else 0.0,
                        "avg_price": float(cols[4].text.replace(',', '')) if cols[4].text.strip() else 0.0,
                        "percentage_change": float(
                            cols[5].text.strip().replace('%', '').replace('.', '').replace(',', '.')) / 100.0 if cols[
                            5].text.strip() else 0.0,
                        "volume": int(float(cols[6].text.replace(',', ''))) if cols[6].text.strip() else 0,
                        "turnover_best": float(cols[7].text.replace(',', '')) if cols[7].text.strip() else 0.0,
                        "total_turnover": float(cols[8].text.replace(',', '')) if cols[8].text.strip() else 0.0,
                    })
            return data
        elif response.status_code == 503:
            print("Service unavailable (503). Retrying...")
            time.sleep(5)
            retry_count += 1
        else:
            print(f"Failed to retrieve data for {symbol}. Status code: {response.status_code}")
            return []

    print("Failed after 5 retries. Exiting.")
    return []


def transform_data(raw_data):
    return [{"date": datetime.strptime(record['date'], "%m/%d/%Y").strftime("%Y-%m-%d"), **record} for record in
            raw_data]


def format_macedonian_number(number):
    return f"{number:,.2f}".replace(",", ".").replace(".", ",", 1)


def store_data(issuer_code, cleaned_data):
    for record in cleaned_data:
        cursor.execute('''
            INSERT OR IGNORE INTO stock_data (issuer_code, date, last_trade_price, max_price, min_price, avg_price, percentage_change, volume, turnover_best, total_turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            issuer_code,
            record['date'],
            format_macedonian_number(record['last_trade_price']),
            format_macedonian_number(record['max_price']),
            format_macedonian_number(record['min_price']),
            format_macedonian_number(record['avg_price']),
            record['percentage_change'],
            record['volume'],
            format_macedonian_number(record['turnover_best']),
            format_macedonian_number(record['total_turnover'])
        ))
    conn.commit()


def get_dates(issuer_code):
    cursor.execute(''' 
        SELECT MAX(date) FROM stock_data WHERE issuer_code = ? 
    ''', (issuer_code,))
    last_date = cursor.fetchone()[0]
    return datetime.strptime(last_date, "%Y-%m-%d") if last_date else None


def main_pipeline():
    start_time = time.time()
    symbols = get_issuer_codes()
    for symbol in symbols:
        last_date = get_dates(symbol)
        if last_date is None:
            start_date = datetime.now() - timedelta(days=365 * 10)
            end_date = datetime.now()
        else:
            start_date = last_date + timedelta(days=1)

        today = datetime.now()
        while start_date < today:
            end_date = min(start_date + timedelta(days=365), today)
            raw_data = get_stock_data(symbol, start_date, end_date)
            if raw_data:
                cleaned_data = transform_data(raw_data)
                store_data(symbol, cleaned_data)
            start_date = end_date + timedelta(days=1)

    exec_time = time.time() - start_time
    print(f"Execution time: {exec_time:.2f} seconds")


main_pipeline()
conn.close()