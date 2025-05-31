import sqlite3
import requests
from bs4 import BeautifulSoup
import datetime
import time
import os
import re
DB_NAME = "maindb.db"
TABLE_NAME_APART = "ceny_skupu_apart"
URL_APART_SKUP = "https://mennica.apart.pl/skup"

def get_db_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two directories to the project root, then into 'databases'
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
    return os.path.join(project_root, 'databases', DB_NAME)

def create_apart_purchase_prices_table(db_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME_APART} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kategoria_produktu TEXT,
                nazwa_produktu TEXT NOT NULL,
                cena_skupu REAL NOT NULL,
                timestamp_pobrania TEXT NOT NULL
            )
        """)
        conn.commit()
        print(f"Tabela '{TABLE_NAME_APART}' sprawdzona/utworzona.")
    except sqlite3.Error as e:
        print(f"Błąd SQLite podczas tworzenia tabeli '{TABLE_NAME_APART}': {e}")
    finally:
        if conn:
            conn.close()

def clean_price(price_str):
    """Czyści string z ceną i konwertuje na float."""
    if price_str is None:
        return None
    cleaned = price_str.replace("zł", "").replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        print(f"Nie udało się przekonwertować ceny: '{price_str}' na liczbę.")
        return None

def scrape_apart_purchase_prices():
    """Scrapuje ceny skupu ze strony Mennicy Apart."""
    print(f"Pobieranie danych ze strony: {URL_APART_SKUP}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(URL_APART_SKUP, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas pobierania strony {URL_APART_SKUP}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    scraped_data = []
    current_timestamp = datetime.datetime.now().isoformat()

    accordion_container = soup.find('div', id='accordionProductDetails')
    if not accordion_container:
        print("Nie znaleziono głównego kontenera 'accordionProductDetails'. Strona mogła zmienić strukturę.")
        return []

    panels = accordion_container.find_all('div', class_='panel panel-default')
    if not panels:
        print("Nie znaleziono paneli z kategoriami produktów.")
        return []

    for panel in panels:
        category_name = "Nieznana kategoria"
        panel_heading = panel.find('div', class_='panel-heading')
        if panel_heading:
            title_tag = panel_heading.find('h4', class_='panel-title')
            if title_tag and title_tag.find('a'):
                category_name = title_tag.find('a').get_text(strip=True)
                indicator_icon = title_tag.find('a').find('i', class_='indicator')
                if indicator_icon:
                    category_name = category_name.replace(indicator_icon.get_text(strip=True), "").strip()

        data_table = panel.find('table', class_=re.compile(r'table\s'))
        if not data_table:
            print(f"Nie znaleziono tabeli danych dla kategorii: {category_name}")
            continue

        tbody = data_table.find('tbody')
        if not tbody:
            print(f"Nie znaleziono tbody w tabeli dla kategorii: {category_name}")
            continue
        
        rows = tbody.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                product_name = cols[0].get_text(strip=True)
                price_str = cols[1].get_text(strip=True)
                
                price_value = clean_price(price_str)

                if product_name and price_value is not None:
                    scraped_data.append((
                        category_name,
                        product_name,
                        price_value,
                        current_timestamp
                    ))
                else:
                    if not product_name:
                        print(f"Pominięto wiersz z pustą nazwą produktu w kategorii '{category_name}'.")
                    if price_value is None and price_str:
                         print(f"Pominięto produkt '{product_name}' z powodu niepoprawnej ceny '{price_str}'.")

    return scraped_data

def insert_apart_data_to_db(db_path, data_list):
    if not data_list:
        print("Brak danych do wstawienia.")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        sql = f"""
            INSERT INTO {TABLE_NAME_APART} (
                kategoria_produktu, nazwa_produktu, cena_skupu, timestamp_pobrania
            ) VALUES (?, ?, ?, ?)
        """
        cursor.executemany(sql, data_list)
        conn.commit()
        print(f"Dodano {cursor.rowcount} wierszy do tabeli '{TABLE_NAME_APART}'.")
    except sqlite3.Error as e:
        print(f"Błąd SQLite podczas wstawiania danych do '{TABLE_NAME_APART}': {e}")
    finally:
        if conn:
            conn.close()

def main():
    db_path = get_db_path()
    create_apart_purchase_prices_table(db_path)

    print(f"\nRozpoczynam scrapowanie cen skupu z Mennicy Apart...")
    apart_prices_data = scrape_apart_purchase_prices()

    if apart_prices_data:
        insert_apart_data_to_db(db_path, apart_prices_data)
    else:
        print("Nie udało się pobrać żadnych danych z Mennicy Apart.")

if __name__ == "__main__":
    main()
