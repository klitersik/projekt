import yfinance as yf
import sqlite3
import datetime
import time
import os

DB_NAME = "C:/Users/maciej.pal/OneDrive - Customs Support/Dokumenty/GitHub/projekt/databases/maindb.db"
TABLE_NAME_YFINANCE = "yfinance_stock_data"

POPULAR_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK-A", "BRK-B", "JPM",
    "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "BAC", "DIS", "PYPL",
    "NFLX", "ADBE", "CRM", "PFE", "KO", "XOM", "CVX", "MCD", "NKE", "INTC",
    "CSCO", "PEP", "COST", "AVGO", "ABBV", "TMO", "ACN", "LLY", "MRK", "ORCL",
]
DEMO_TICKERS = POPULAR_TICKERS[:15]

def get_db_path():
    """Zwraca pełną ścieżkę do pliku bazy danych."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, DB_NAME)

def create_yfinance_table(db_path):
    """Tworzy tabelę dla danych z yfinance, jeśli jeszcze nie istnieje."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME_YFINANCE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                data_notowania DATE NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                adj_close_price REAL,
                volume INTEGER,
                UNIQUE(ticker, data_notowania) -- Zapobiega duplikatom dla tego samego tickera i dnia
            )
        """)
        conn.commit()
        print(f"Tabela '{TABLE_NAME_YFINANCE}' sprawdzona/utworzona.")
    except sqlite3.Error as e:
        print(f"Błąd SQLite podczas tworzenia tabeli '{TABLE_NAME_YFINANCE}': {e}")
    finally:
        if conn:
            conn.close()

def fetch_stock_data_for_tickers(tickers_list, start_date_str, end_date_str):
    """Pobiera dane historyczne dla listy tickerów z yfinance."""
    all_stock_data = []
    print(f"Pobieranie danych dla {len(tickers_list)} tickerów od {start_date_str} do {end_date_str}...")

    for i, ticker_symbol in enumerate(tickers_list):
        print(f"  Pobieranie {ticker_symbol} ({i+1}/{len(tickers_list)})...")
        try:
            ticker_data = yf.Ticker(ticker_symbol)
            hist = ticker_data.history(start=start_date_str, end=end_date_str)

            if hist.empty:
                print(f"    Brak danych dla {ticker_symbol} w podanym okresie.")
                continue

            for date_index, row in hist.iterrows():
                trade_date = date_index.strftime('%Y-%m-%d')
                
                all_stock_data.append((
                    ticker_symbol,
                    trade_date,
                    row.get('Open'),
                    row.get('High'),
                    row.get('Low'),
                    row.get('Close'),
                    row.get('Adj Close'),
                    row.get('Volume')
                ))
            
            time.sleep(0.5)

        except Exception as e:
            print(f"    Wystąpił błąd podczas pobierania danych dla {ticker_symbol}: {e}")

    return all_stock_data

def insert_yfinance_data(db_path, stock_data_list):
    if not stock_data_list:
        print("Brak danych do wstawienia do bazy.")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        sql = f"""
            INSERT OR IGNORE INTO {TABLE_NAME_YFINANCE} (
                ticker, data_notowania, open_price, high_price, low_price,
                close_price, adj_close_price, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(sql, stock_data_list)
        conn.commit()
        print(f"Dodano/zignorowano {cursor.rowcount} wierszy do tabeli '{TABLE_NAME_YFINANCE}'.")
    except sqlite3.Error as e:
        print(f"Błąd SQLite podczas wstawiania danych do '{TABLE_NAME_YFINANCE}': {e}")
    finally:
        if conn:
            conn.close()

def main():
    db_path = get_db_path()
    create_yfinance_table(db_path)
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=7)

    end_date_str = end_date.strftime('%Y-%m-%d')
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    tickers_to_fetch = DEMO_TICKERS
    print(f"Docelowa liczba tickerów: {len(tickers_to_fetch)} (lista może być skrócona do celów demo).")


    print(f"\nRozpoczynam pobieranie danych giełdowych z yfinance...")
    historical_data = fetch_stock_data_for_tickers(tickers_to_fetch, start_date_str, end_date_str)

    if historical_data:
        insert_yfinance_data(db_path, historical_data)
    else:
        print("Nie udało się pobrać żadnych danych giełdowych.")

if __name__ == "__main__":
    main()