# sync_trends.py
import requests
import psycopg2
from datetime import datetime, timezone
import warnings
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

warnings.filterwarnings('ignore')
requests.packages.urllib3.disable_warnings()

# === Настройки ===
ZABBIX_URL = os.environ.get("ZABBIX_URL")
API_TOKEN = os.environ.get("ZABBIX_API_TOKEN")

DB_CONFIG = {
    "dbname": os.environ.get("DB_DBNAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": "5432"
}

DAYS_HISTORY = 365
MAX_WORKERS = 4

def fetch_from_zabbix(itemid, time_from, time_till):
    headers = {"Content-Type": "application/json-rpc", "Authorization": f"Bearer {API_TOKEN}"}
    data = {
        "jsonrpc": "2.0",
        "method": "trend.get",
        "params": {
            "output": ["clock", "value_min", "value_avg", "value_max"],
            "itemids": [itemid],
            "time_from": time_from,
            "time_till": time_till,
            "limit": 10000
        },
        "id": 1
    }
    resp = requests.post(ZABBIX_URL.strip(), headers=headers, json=data, verify=False, timeout=60)
    if resp.status_code != 200 or 'error' in resp.json():
        raise Exception(f"Zabbix error for {itemid}: {resp.json().get('error', 'Unknown')}")
    return resp.json()['result']

def update_total_for_item(used_itemid, total_itemid):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("SELECT updated_at FROM disk_total WHERE used_itemid = %s", (used_itemid,))
        row = cur.fetchone()
        should_update = False
        if row is None:
            should_update = True
        else:
            last_update = row[0]
            if (datetime.now(timezone.utc) - last_update).days >= 1:
                should_update = True

        if should_update:
            time_till = int(datetime.now().timestamp())
            time_from = time_till - 7 * 86400
            trends = fetch_from_zabbix(total_itemid, time_from, time_till)
            if trends:
                latest = max(trends, key=lambda x: int(x['clock']))
                total_bytes = float(latest['value_avg'])
                cur.execute("""
                    INSERT INTO disk_total (used_itemid, total_itemid, total_bytes, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (used_itemid) DO UPDATE
                    SET total_bytes = EXCLUDED.total_bytes, updated_at = NOW()
                """, (used_itemid, total_itemid, total_bytes))
                conn.commit()
    except Exception as e:
        pass  # Убираем вывод ошибок для чистоты
    finally:
        cur.close()
        conn.close()

def sync_single_disk(used_itemid, total_itemid, cutoff_clock):
    """Синхронизирует один диск и агрегирует до дней"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    daily_records = []

    try:
        # Находим последнюю точку ПОСЛЕ cutoff
        cur.execute("""
            SELECT MAX(clock) FROM zabbix_trends 
            WHERE itemid = %s AND clock >= %s
        """, (used_itemid, cutoff_clock))
        last_clock = cur.fetchone()[0]

        if last_clock is None:
            time_from = cutoff_clock
        else:
            time_from = last_clock + 1

        time_till = int(datetime.now().timestamp())

        if time_from < time_till:
            trends = fetch_from_zabbix(used_itemid, time_from, time_till)
            if trends:
                # Агрегация до дней
                df = pd.DataFrame(trends)
                df['clock'] = df['clock'].astype(int)
                df['value_avg'] = pd.to_numeric(df['value_avg'], errors='coerce')
                df['value_min'] = pd.to_numeric(df['value_min'], errors='coerce')
                df['value_max'] = pd.to_numeric(df['value_max'], errors='coerce')
                df['date'] = pd.to_datetime(df['clock'], unit='s').dt.date
                
                # Группируем по дням
                daily_agg = df.groupby('date').agg({
                    'value_avg': 'mean',
                    'value_min': 'min',
                    'value_max': 'max'
                }).reset_index()
                
                # Преобразуем обратно в Unix timestamp (начало дня)
                for _, row in daily_agg.iterrows():
                    day_start = int(datetime.combine(row['date'], datetime.min.time()).timestamp())
                    daily_records.append((
                        str(used_itemid),
                        day_start,
                        row['value_avg'],
                        row['value_min'],
                        row['value_max']
                    ))

        update_total_for_item(used_itemid, total_itemid)

    except Exception:
        pass  # Убираем ошибки для production
    finally:
        cur.close()
        conn.close()

    return daily_records

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT itemid, total_itemid 
        FROM prediction_targets 
        WHERE enabled = true
    """)
    targets = cur.fetchall()
    itemids = [str(item[0]) for item in targets]
    cur.close()
    conn.close()

    if not itemids:
        return

    # Очистка старых данных
    cutoff_clock = int(datetime.now().timestamp()) - DAYS_HISTORY * 86400
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM zabbix_trends 
        WHERE itemid = ANY(%s) AND clock < %s
    """, (itemids, cutoff_clock))
    conn.commit()
    cur.close()
    conn.close()

    # Параллельная синхронизация
    all_records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_itemid = {
            executor.submit(sync_single_disk, used_id, total_id, cutoff_clock): used_id
            for used_id, total_id in targets
        }

        for future in as_completed(future_to_itemid):
            records = future.result()
            all_records.extend(records)

    # Bulk insert
    if all_records:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO zabbix_trends (itemid, clock, value_avg, value_min, value_max)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (itemid, clock) DO NOTHING
        """
        cur.executemany(insert_query, all_records)
        conn.commit()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
