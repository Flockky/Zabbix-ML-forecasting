# demo_forecast.py
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import psycopg2
from neuralprophet import NeuralProphet
import logging
import torch
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import warnings

# === ТИШИНА ===
warnings.filterwarnings('ignore')
logging.getLogger("pytorch_lightning").setLevel(logging.ERROR)
logging.getLogger("lightning.pytorch").setLevel(logging.ERROR)
logging.getLogger("NP").setLevel(logging.ERROR)
logging.getLogger("neuralprophet").setLevel(logging.ERROR)
logging.getLogger("matplotlib").setLevel(logging.ERROR)
os.environ["PYTHONWARNINGS"] = "ignore"

# === CPU ===
os.environ["OMP_NUM_THREADS"] = str(os.cpu_count())
os.environ["OPENBLAS_NUM_THREADS"] = str(os.cpu_count())
os.environ["MKL_NUM_THREADS"] = str(os.cpu_count())
torch.set_num_threads(os.cpu_count())
torch.set_num_interop_threads(os.cpu_count())

DB_CONFIG = {
    "dbname": os.environ.get("DB_DBNAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": "5432"
}

DAYS_BACK = 60
FORECAST_DAYS = 90
HISTORY_DAYS = 365

def detect_cleanups(df, total_bytes, drop_pct_threshold=10.0):
    df = df.sort_values('ds').reset_index(drop=True)
    if len(df) < 2:
        return None
    df['prev'] = df['y'].shift(1)
    df['drop_bytes'] = df['prev'] - df['y']
    df['drop_pct'] = (df['drop_bytes'] / total_bytes) * 100
    cleanups = df[df['drop_pct'] >= drop_pct_threshold]
    return cleanups['ds'].max() if not cleanups.empty else None

def demo_single_disk(itemid, db_config):
    """Создаёт демо-прогноз для одного диска"""
    try:
        itemid_str = str(itemid)
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Определяем cutoff дату (7 дней назад)
        cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
        cutoff_clock = int(cutoff_date.timestamp())
        
        # Загружаем исторические данные ДО cutoff даты
        historical_cutoff = int(datetime.now().timestamp()) - HISTORY_DAYS * 86400
        cur.execute("""
            SELECT clock, value_avg 
            FROM zabbix_trends 
            WHERE itemid = %s AND value_avg IS NOT NULL AND clock >= %s AND clock <= %s
            ORDER BY clock
        """, (itemid_str, historical_cutoff, cutoff_clock))
        used_data = cur.fetchall()
        
        if not used_data or len(used_data) < 2:
            cur.close()
            conn.close()
            return False

        df = pd.DataFrame(used_data, columns=['ds', 'y'])
        df['ds'] = pd.to_datetime(df['ds'], unit='s')
        df = df.dropna().sort_values('ds').reset_index(drop=True)

        if df['y'].nunique() < 2:
            cur.close()
            conn.close()
            return False

        # Получение total_bytes
        cur.execute("SELECT total_bytes FROM disk_total WHERE used_itemid = %s", (itemid_str,))
        total_row = cur.fetchone()
        if total_row is None:
            cur.close()
            conn.close()
            return False
        total_bytes = float(total_row[0])

        # Обнаружение очисток
        cleanup_date = detect_cleanups(df, total_bytes, 10.0)
        if cleanup_date is not None:
            window_start = cleanup_date - timedelta(days=1)
            window_end = cleanup_date + timedelta(days=1)
            window = df[(df['ds'] >= window_start) & (df['ds'] <= window_end)]
            if not window.empty:
                anomaly_row = window.loc[window['y'].idxmin()]
                df = df[df['ds'] != anomaly_row['ds']]

        # Обучение модели
        m = NeuralProphet(
            yearly_seasonality=False,
            weekly_seasonality=True,
            daily_seasonality=False,
            growth='linear',
            epochs=30,
            batch_size=64,
            learning_rate=1.0
        )
        m.fit(df, freq='D')

        # Прогноз на 97 дней (7 + 90)
        future = m.make_future_dataframe(df, periods=DAYS_BACK + FORECAST_DAYS)
        forecast = m.predict(future)

        # Получаем реальные данные за последние 7 дней
        today_clock = int(datetime.now().timestamp())
        cur.execute("""
            SELECT clock, value_avg 
            FROM zabbix_trends 
            WHERE itemid = %s AND clock > %s AND clock <= %s
            ORDER BY clock
        """, (itemid_str, cutoff_clock, today_clock))
        actual_data = cur.fetchall()
        
        # Преобразуем реальные данные в DataFrame
        actual_df = pd.DataFrame(actual_data, columns=['actual_ds', 'actual_y'])
        actual_df['actual_ds'] = pd.to_datetime(actual_df['actual_ds'], unit='s')

        # Сохраняем в demo_forecasts
        run_id = str(datetime.now(timezone.utc).timestamp())
        
        # Удаляем старые записи для этого диска
        cur.execute("DELETE FROM demo_forecasts WHERE itemid = %s", (itemid_str,))
        
        # Подготавливаем записи для вставки
        records_to_insert = []
        for _, row in forecast.iterrows():
            ds_timestamp = row['ds'].to_pydatetime()
            yhat_value = float(row['yhat1']) if pd.notna(row['yhat1']) else None
            
            # Ищем реальное значение
            actual_match = actual_df[actual_df['actual_ds'] == row['ds']]
            actual_value = None
            if not actual_match.empty:
                actual_value = float(actual_match.iloc[0]['actual_y'])
            
            records_to_insert.append((
                itemid_str,
                ds_timestamp,
                yhat_value,
                actual_value,
                run_id
            ))
        
        # Вставляем все записи
        insert_query = """
            INSERT INTO demo_forecasts (itemid, ds, yhat, actual_y, run_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.executemany(insert_query, records_to_insert)
        conn.commit()
        cur.close()
        conn.close()
        return True

    except Exception:
        try:
            cur.close()
            conn.close()
        except:
            pass
        return False

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT itemid FROM prediction_targets WHERE enabled = true
    """)
    targets = cur.fetchall()
    cur.close()
    conn.close()

    if not targets:
        print("⚠️ Нет активных дисков")
        return

    completed = 0
    max_workers = min(multiprocessing.cpu_count(), 8)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_itemid = {
            executor.submit(demo_single_disk, itemid, DB_CONFIG): (itemid, i)
            for i, (itemid,) in enumerate(targets, 1)
        }
        
        for future in as_completed(future_to_itemid):
            itemid, i = future_to_itemid[future]
            try:
                success = future.result(timeout=300)
                if success:
                    completed += 1
                    print(f"   ✅ [{i}/{len(targets)}] {itemid}")
                else:
                    print(f"   ⚠️  [{i}/{len(targets)}] {itemid} — пропущен")
            except Exception:
                print(f"   ❌ [{i}/{len(targets)}] {itemid} — ошибка")

    print(f"\n📊 Демо-прогнозы созданы: {completed}/{len(targets)} дисков")

if __name__ == "__main__":
    main()
