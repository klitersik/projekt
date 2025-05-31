import sqlite3
import os

def stworz_baze_danych(nazwa_bazy="maindb.db"):
    try:
        sciezka_skryptu = os.path.dirname(os.path.abspath(__file__))
        pelna_sciezka_bazy = os.path.join(sciezka_skryptu, nazwa_bazy)

        with sqlite3.connect(pelna_sciezka_bazy) as conn:
            print(f"Baza danych '{nazwa_bazy}' została pomyślnie utworzona/otwarta.")
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS przyklady (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nazwa TEXT NOT NULL,
                    wartosc REAL
                )
            ''')
            conn.commit()
            print("Tabela 'przyklady' została utworzona (jeśli nie istniała).")

    except sqlite3.Error as e:
        print(f"Wystąpił błąd SQLite: {e}")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd: {e}")

if __name__ == "__main__":
    nazwa_pliku_bazy = "maindb.db"
    stworz_baze_danych(nazwa_pliku_bazy)