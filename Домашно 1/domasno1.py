import requests
import sqlite3
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, Lock
import time
import os


db_path = os.path.join(os.path.dirname(__file__), 'crypto_data.db')
db_lock = Lock()


def filter1_get_top1000():
    url = "https://api.coingecko.com/api/v3/coins/markets"

    cryptos = []
    for page in range(1, 5):  # 4 pages * 250 = 1000
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page
        }
        r = requests.get(url, params=params)
        data = r.json()

        for coin in data:
            cryptos.append({
                "id": coin["id"],
                "symbol": coin["symbol"].upper(),
                "name": coin["name"]
            })

    return cryptos[:1000]



def filter2_check_last_date(crypto_id):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crypto_prices (
                crypto_id TEXT,
                symbol TEXT,
                name TEXT,
                source TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (crypto_id, source, date)
            )
        ''')

        cursor.execute("""
            SELECT MAX(date) FROM crypto_prices WHERE crypto_id = ?
        """, (crypto_id,))

        result = cursor.fetchone()
        return result[0] if result[0] else "2015-01-01"



def filter3_fetch_binance(crypto, last_date):
    symbol = crypto["symbol"] + "USDT"

    url = "https://api.binance.com/api/v3/klines"

    start_ms = int(datetime.strptime(last_date, "%Y-%m-%d").timestamp()) * 1000

    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": start_ms,
        "limit": 1000
    }

    response = requests.get(url, params=params)
    raw = response.json()

    if isinstance(raw, dict) and "code" in raw:
        return pd.DataFrame()  # symbol not available on Binance

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote", "trades", "tb_base", "tb_quote", "ignore"
    ])

    df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.strftime("%Y-%m-%d")
    df = df[["date", "open", "high", "low", "close", "volume"]]

    df["source"] = "binance"
    return df



def filter4_save_to_db(df, crypto):
    if df.empty:
        return

    with db_lock:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            rows = [
                (
                    crypto["id"], crypto["symbol"], crypto["name"], row["source"],
                    row["date"], row["open"], row["high"], row["low"],
                    row["close"], row["volume"]
                )
                for _, row in df.iterrows()
            ]

            cursor.executemany('''
                INSERT OR IGNORE INTO crypto_prices
                (crypto_id, symbol, name, source, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            conn.commit()


def process_crypto(crypto):
    last_date = filter2_check_last_date(crypto["id"])

    df_binance = filter3_fetch_binance(crypto, last_date)

    if not df_binance.empty:
        filter4_save_to_db(df_binance, crypto)
        print(f"{crypto['symbol']} saved {len(df_binance)} Binance rows.")
    else:
        print(f"{crypto['symbol']} has no Binance data.")



def pipeline():
    cryptos = filter1_get_top1000()
    print(f"Loaded {len(cryptos)} cryptos.")

    with Pool(processes=3) as pool:
        pool.map(process_crypto, cryptos)



def main():
    start = time.time()
    pipeline()
    end = time.time()
    print(f"Finished in {end - start:.2f} seconds.")


if __name__ == "__main__":
    main()
