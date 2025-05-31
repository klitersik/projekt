import sqlite3
import requests
import datetime
import time
import os
import json

DB_NAME = "C:/Users/maciej.pal/OneDrive - Customs Support/Dokumenty/GitHub/projekt/databases/maindb.db"
TABLE_NAME_NBP = "kursy_walut_nbp"

def get_db_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, DB_NAME)

def create_nbp_currency_table(db_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME_NBP} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_notowania DATE NOT NULL,
                nazwa_waluty TEXT NOT NULL,
                kod_waluty TEXT NOT NULL,
                kurs_sredni REAL NOT NULL,
                UNIQUE(data_notowania, kod_waluty) -- Zapobiega duplikatom dla tej samej waluty i dnia
            )
        """)
        conn.commit()
        print(f"Tabela '{TABLE_NAME_NBP}' sprawdzona/utworzona.")
    except sqlite3.Error as e:
        print(f"Błąd SQLite podczas tworzenia tabeli '{TABLE_NAME_NBP}': {e}")
    finally:
        if conn:
            conn.close()

def fetch_nbp_rates_for_date(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    url = f"http://api.nbp.pl/api/exchangerates/tables/A/{date_str}/?format=json"
    print(f"Pobieranie kursów walut NBP dla {date_str} z {url}...")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if not data or not isinstance(data, list) or not data[0].get('rates'):
            print(f"Otrzymano nieoczekiwany format danych dla {date_str}.")
            return []
            
        rates_data = []
        for rate_info in data[0]['rates']:
            rates_data.append((
                date_str,                   
                rate_info['currency'],      
                rate_info['code'],          
                float(rate_info['mid'])   
            ))
        return rates_data
        
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            print(f"Brak danych (404) dla {date_str} w NBP API (prawdopodobnie weekend lub święto).")
        else:
            print(f"Błąd HTTP podczas pobierania danych NBP dla {date_str}: {http_err}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Błąd połączenia podczas pobierania danych NBP dla {date_str}: {e}")
        return []
    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
        print(f"Błąd przetwarzania danych JSON z NBP API dla {date_str}: {e}")
        return []


def insert_nbp_currency_data(db_path, currency_data_list):
    if not currency_data_list:
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        sql = f"""
            INSERT OR IGNORE INTO {TABLE_NAME_NBP} (
                data_notowania, nazwa_waluty, kod_waluty, kurs_sredni
            ) VALUES (?, ?, ?, ?)
        """
        cursor.executemany(sql, currency_data_list)
        conn.commit()
        print(f"Dodano/zignorowano {cursor.rowcount} wierszy do tabeli '{TABLE_NAME_NBP}'.")
    except sqlite3.Error as e:
        print(f"Błąd SQLite podczas wstawiania danych do '{TABLE_NAME_NBP}': {e}")
    finally:
        if conn:
            conn.close()

def main():
    db_path = get_db_path()
    create_nbp_currency_table(db_path)

    today = datetime.date.today()
    print(f"\nRozpoczynam pobieranie danych kursów walut NBP za ostatnie 7 dni (do {today.strftime('%Y-%m-%d')}).")
    for i in range(7):
        target_date = today - datetime.timedelta(days=i)
        
        nbp_rates = fetch_nbp_rates_for_date(target_date)
        if nbp_rates:
            insert_nbp_currency_data(db_path, nbp_rates)
        time.sleep(0.2)

if __name__ == "__main__":
    main()