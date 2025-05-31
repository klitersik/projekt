import sqlite3
import os
import hashlib
import flet as ft
import datetime

DATABASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'databases', 'maindb.db')

NBP_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'databases', 'maindb.db')
APART_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'databases', 'maindb.db')
YFINANCE_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'databases', 'maindb.db')

TABLE_NAME_NBP = "kursy_walut_nbp"
TABLE_NAME_APART = "ceny_skupu_apart"
TABLE_NAME_YFINANCE = "yfinance_stock_data"

DEFAULT_CURRENCY_PRICE = 4.0

def init_database():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            asset_name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    conn.commit()
    conn.close()

def get_db_connection():
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    init_database()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(username, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        hashed_password = hash_password(password)
        cursor.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, hashed_password))
        conn.commit()
        return True, "Użytkownik zarejestrowany pomyślnie."
    except sqlite3.IntegrityError:
        return False, "Błąd: Użytkownik już istnieje."
    finally:
        conn.close()

def login_user(username, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hash_password(password)
    cursor.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, hashed_password))
    user = cursor.fetchone()
    conn.close()
    if user:
        return user['id'], "Zalogowano pomyślnie."
    else:
        return None, "Błąd logowania: Nieprawidłowa nazwa użytkownika lub hasło."

def add_asset_to_portfolio_db(user_id, asset_name, asset_type, quantity):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO portfolios (user_id, asset_name, asset_type, quantity) VALUES (?, ?, ?, ?)",
                       (user_id, asset_name.upper(), asset_type, quantity))
        conn.commit()
        return True, f"Aktyw '{asset_name.upper()}' dodany do portfolio."
    except sqlite3.Error as e:
        return False, f"Błąd podczas dodawania aktywa do portfolio: {e}"
    finally:
        conn.close()

def get_user_portfolio(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT asset_name, asset_type, quantity FROM portfolios WHERE user_id = ?", (user_id,))
    assets = cursor.fetchall()
    conn.close()
    return assets

def get_asset_price(asset_name, asset_type):
    price = 0
    
    if asset_type == 'currency':
        price = DEFAULT_CURRENCY_PRICE
        
    elif asset_type == 'gold':
        try:
            conn = sqlite3.connect(APART_DB_PATH)
            cursor = conn.cursor()
            cursor.execute(f"SELECT cena_skupu FROM {TABLE_NAME_APART} WHERE nazwa_produktu = ? ORDER BY timestamp_pobrania DESC LIMIT 1", (asset_name,))
            result = cursor.fetchone()
            if result:
                price = result[0]
            else:
                price = 200.0
        except sqlite3.Error:
            price = 200.0
        finally:
            if 'conn' in locals():
                conn.close()
                
    elif asset_type == 'stock':
        try:
            conn = sqlite3.connect(YFINANCE_DB_PATH)
            cursor = conn.cursor()
            cursor.execute(f"SELECT close_price FROM {TABLE_NAME_YFINANCE} WHERE ticker = ? ORDER BY data_notowania DESC LIMIT 1", (asset_name,))
            result = cursor.fetchone()
            if result:
                price = result[0]
            else:
                price = 100.0
        except sqlite3.Error:
            price = 100.0
        finally:
            if 'conn' in locals():
                conn.close()
    
    return price

def get_portfolio_value_and_categories(user_id):
    assets = get_user_portfolio(user_id)
    if not assets:
        return 0, {"currency": 0, "gold": 0, "stock": 0}, {}

    category_values = {"currency": 0, "gold": 0, "stock": 0}
    assets_by_category = {"currency": [], "gold": [], "stock": []}
    
    total_portfolio_value = 0
    
    for asset in assets:
        current_price = get_asset_price(asset['asset_name'], asset['asset_type'])
        asset_value = asset['quantity'] * current_price

        category_values[asset['asset_type']] += asset_value
        total_portfolio_value += asset_value

        assets_by_category[asset['asset_type']].append({
            "name": asset['asset_name'],
            "quantity": asset['quantity'],
            "price": current_price,
            "value": asset_value
        })

    return total_portfolio_value, category_values, assets_by_category

def get_available_currencies():
    try:
        conn = sqlite3.connect(NBP_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT kod_waluty FROM {TABLE_NAME_NBP} ORDER BY kod_waluty")
        currencies = [row[0] for row in cursor.fetchall()]
        if currencies:
            return currencies
    except sqlite3.Error:
        pass
    finally:
        if 'conn' in locals():
            conn.close()
    
    return ["USD", "EUR", "GBP", "CHF", "JPY", "CAD", "AUD"]

def get_available_gold_products():
    try:
        conn = sqlite3.connect(APART_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT nazwa_produktu FROM {TABLE_NAME_APART} ORDER BY nazwa_produktu")
        products = [row[0] for row in cursor.fetchall()]
        if products:
            return products
    except sqlite3.Error:
        pass
    finally:
        if 'conn' in locals():
            conn.close()
    
    return ["1oz Gold Coin", "10g Gold Bar", "1g Gold Bar"]

def get_available_stock_tickers():
    try:
        conn = sqlite3.connect(YFINANCE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT ticker FROM {TABLE_NAME_YFINANCE} ORDER BY ticker")
        tickers = [row[0] for row in cursor.fetchall()]
        if tickers:
            return tickers
    except sqlite3.Error:
        pass
    finally:
        if 'conn' in locals():
            conn.close()
    
    return ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA"]

def main(page: ft.Page):
    page.title = "INVESTMENT - Aplikacja do zarządzania portfolio"
    page.vertical_alignment = ft.MainAxisAlignment.CENTER
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.window.width = 900
    page.window.height = 700
    page.theme_mode = ft.ThemeMode.LIGHT

    current_user_id = None

    def show_message(message, color=ft.Colors.GREEN_500):
        snack_bar = ft.SnackBar(
            ft.Text(message),
            bgcolor=color
        )
        page.overlay.append(snack_bar)
        snack_bar.open = True
        page.update()

    def login_view():
        username_field = ft.TextField(label="Nazwa użytkownika", width=300)
        password_field = ft.TextField(label="Hasło", password=True, can_reveal_password=True, width=300)
        
        def on_login_click(e):
            nonlocal current_user_id
            if not username_field.value or not password_field.value:
                show_message("Wypełnij wszystkie pola!", ft.Colors.RED_500)
                return
                
            user_id, message = login_user(username_field.value, password_field.value)
            if user_id:
                current_user_id = user_id
                show_message(message)
                page.clean()
                page.add(portfolio_view())
                page.update()
            else:
                show_message(message, ft.Colors.RED_500)

        def on_register_click(e):
            if not username_field.value or not password_field.value:
                show_message("Wypełnij wszystkie pola!", ft.Colors.RED_500)
                return
                
            success, message = register_user(username_field.value, password_field.value)
            if success:
                show_message(message)
                username_field.value = ""
                password_field.value = ""
                page.update()
            else:
                show_message(message, ft.Colors.RED_500)

        return ft.Column(
            [
                ft.Image(src=f"logo.png",width=250,height=250,fit=ft.ImageFit.CONTAIN,),
                ft.Text("Portfolio Manager", size=32, weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_600),
                ft.Text("Zaloguj się lub Zarejestruj", size=18),
                ft.Container(height=20),
                username_field,
                password_field,
                ft.Container(height=20),
                ft.Row(
                    [
                        ft.ElevatedButton("Zaloguj", on_click=on_login_click, width=140),
                        ft.ElevatedButton("Zarejestruj", on_click=on_register_click, width=140),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER
                )
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            alignment=ft.MainAxisAlignment.CENTER
        )

    def portfolio_view():
        asset_type_dropdown = ft.Dropdown(
            label="Kategoria Aktywa",
            options=[
                ft.dropdown.Option("currency", text="Waluty"),
                ft.dropdown.Option("gold", text="Złoto"),
                ft.dropdown.Option("stock", text="Akcje"),
            ],
            width=300
        )
        asset_selection_dropdown = ft.Dropdown(
            label="Wybierz Aktywo",
            width=300,
            options=[]
        )
        quantity_field = ft.TextField(
            label="Ilość", 
            width=300, 
            input_filter=ft.InputFilter(allow=True, regex_string=r"[0-9\.]", replacement_string="")
        )
        
        def update_asset_selection_dropdown(e):
            selected_type = asset_type_dropdown.value
            asset_selection_dropdown.options.clear()
            if selected_type == "currency":
                available_assets = get_available_currencies()
            elif selected_type == "gold":
                available_assets = get_available_gold_products()
            elif selected_type == "stock":
                available_assets = get_available_stock_tickers()
            else:
                available_assets = []
            
            for asset in available_assets:
                asset_selection_dropdown.options.append(ft.dropdown.Option(asset))
            
            asset_selection_dropdown.value = None
            page.update()

        asset_type_dropdown.on_change = update_asset_selection_dropdown

        total_value_text = ft.Text("", size=20, weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_800)
        pie_chart_container = ft.Container()
        portfolio_details_container = ft.Column([],scroll=ft.ScrollMode.ALWAYS)
        
        def create_pie_chart(category_values):
            sections = []
            colors = [ft.Colors.BLUE_500, ft.Colors.AMBER_500, ft.Colors.GREEN_500]
            category_names = {"currency": "Waluty", "gold": "Złoto", "stock": "Akcje"}
            
            total_chart_value = sum(category_values.values())
            if total_chart_value == 0:
                return ft.Container(
                    content=ft.Text("Brak aktywów w portfolio", size=16, color=ft.Colors.GREY_600),
                    alignment=ft.alignment.center,
                    height=200
                )

            for i, (category, value) in enumerate(category_values.items()):
                if value > 0:
                    percentage = (value / total_chart_value) * 100
                    sections.append(
                        ft.PieChartSection(
                            value=value,
                            color=colors[i],
                            title=f"{category_names[category]}\n{percentage:.1f}%",
                            title_style=ft.TextStyle(size=12, color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD),
                        )
                    )
            
            return ft.PieChart(
                sections=sections,
                sections_space=2,
                center_space_radius=50,
                width=250,
                height=250,
            )

        def create_portfolio_details(assets_by_category):
            details = []
            category_names = {"currency": "💰 Waluty", "gold": "🥇 Złoto", "stock": "📈 Akcje"}
            
            for category, assets in assets_by_category.items():
                if assets:
                    details.append(
                        ft.Container(
                            content=ft.Text(
                                category_names[category], 
                                size=18, 
                                weight=ft.FontWeight.BOLD,
                                color=ft.Colors.BLUE_700
                            ),
                            padding=ft.padding.only(top=20, bottom=10)
                        )
                    )
                    
                    for asset in assets:
                        details.append(
                            ft.Container(
                                content=ft.Row([
                                    ft.Text(
                                        f"• {asset['name']}", 
                                        size=14, 
                                        weight=ft.FontWeight.W_500,
                                        expand=2
                                    ),
                                    ft.Text(
                                        f"{asset['quantity']:.2f} szt.", 
                                        size=14,
                                        expand=1
                                    ),
                                    ft.Text(
                                        f"{asset['price']:.2f} PLN", 
                                        size=14,
                                        expand=1
                                    ),
                                    ft.Text(
                                        f"{asset['value']:.2f} PLN", 
                                        size=14, 
                                        weight=ft.FontWeight.BOLD,
                                        color=ft.Colors.GREEN_700,
                                        expand=1
                                    ),
                                ]),
                                bgcolor=ft.Colors.GREY_100,
                                border_radius=5,
                                padding=10,
                                margin=ft.margin.only(bottom=5)
                            )
                        )
            
            if not details:
                details.append(
                    ft.Container(
                        content=ft.Text("Twoje portfolio jest puste. Dodaj pierwsze aktywa!", 
                                       size=16, color=ft.Colors.GREY_600),
                        alignment=ft.alignment.center,
                        padding=20
                    )
                )
            
            return details

        def update_portfolio_display():
            total_value, category_values, assets_by_category = get_portfolio_value_and_categories(current_user_id)
            
            total_value_text.value = f"💼 Całkowita wartość portfolio: {total_value:.2f} PLN"
            pie_chart_container.content = create_pie_chart(category_values)
            
            portfolio_details_container.controls.clear()
            portfolio_details_container.controls.extend(create_portfolio_details(assets_by_category))
            
            page.update()

        def on_add_asset_click(e):
            asset_name = asset_selection_dropdown.value
            quantity_str = quantity_field.value
            asset_type = asset_type_dropdown.value

            if not asset_name or not quantity_str or not asset_type:
                show_message("Wypełnij wszystkie pola!", ft.Colors.RED_500)
                return
            
            try:
                quantity = float(quantity_str)
                if quantity <= 0:
                    show_message("Ilość musi być większa od zera.", ft.Colors.RED_500)
                    return
            except ValueError:
                show_message("Nieprawidłowa ilość. Podaj liczbę.", ft.Colors.RED_500)
                return

            success, message = add_asset_to_portfolio_db(current_user_id, asset_name, asset_type, quantity)
            if success:
                show_message(message)
                asset_selection_dropdown.value = None
                quantity_field.value = ""
                asset_type_dropdown.value = None
                asset_selection_dropdown.options.clear()
                update_portfolio_display()
            else:
                show_message(message, ft.Colors.RED_500)

        update_portfolio_display()

        return ft.Column(
            [
                ft.Container(
                    content=ft.Row([
                        ft.Text("📊 Zarządzanie Portfolio", size=28, weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_700),
                        ft.IconButton(
                            icon=ft.Icons.LOGOUT,
                            tooltip="Wyloguj",
                            on_click=lambda e: (page.clean(), page.add(login_view()), page.update())
                        )
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    padding=ft.padding.only(bottom=20)
                ),
                
                ft.Container(
                    content=ft.Column([
                        ft.Text("➕ Dodaj nowe aktywo", size=18, weight=ft.FontWeight.BOLD),
                        ft.Row([
                            asset_type_dropdown,
                            asset_selection_dropdown,
                            quantity_field,
                        ], alignment=ft.MainAxisAlignment.CENTER),
                        ft.ElevatedButton(
                            "Dodaj Aktywo", 
                            on_click=on_add_asset_click,
                            style=ft.ButtonStyle(
                                bgcolor=ft.Colors.BLUE_600,
                                color=ft.Colors.WHITE
                            )
                        ),
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    bgcolor=ft.Colors.BLUE_50,
                    border_radius=10,
                    padding=20,
                    margin=ft.margin.only(bottom=20)
                ),
                
                total_value_text,
                ft.Container(height=20),
                
                ft.Row([
                    ft.Container(
                        content=ft.Column([
                            ft.Text("📊 Struktura Portfolio", size=16, weight=ft.FontWeight.BOLD),
                            pie_chart_container,
                        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                        expand=1,
                        padding=20
                    ),
                    
                    ft.Container(
                        content=ft.Column([
                            ft.Text("📋 Szczegóły Portfolio", size=16, weight=ft.FontWeight.BOLD),
                            ft.Column([
                                portfolio_details_container
                            ], scroll=ft.ScrollMode.AUTO, height=300)
                        ]),
                        expand=2,
                        padding=20
                    ),
                ], alignment=ft.MainAxisAlignment.START),
                
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            scroll=ft.ScrollMode.AUTO,
            expand=True
        )

    page.add(login_view())

if __name__ == "__main__":
    ft.app(target=main)