# run_forecast.py
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

DAYS_FORECAST = 90
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

def forecast_single_disk(itemid, total_itemid, threshold_pct, db_config):
    """Прогноз с NeuralProphet без годовой сезонности"""
    try:
        itemid_str = str(itemid)
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Теперь данные уже дневные!
        cutoff_clock = int(datetime.now().timestamp()) - HISTORY_DAYS * 86400
        cur.execute("""
            SELECT clock, value_avg 
            FROM zabbix_trends 
            WHERE itemid = %s AND value_avg IS NOT NULL AND clock >= %s
            ORDER BY clock
        """, (itemid_str, cutoff_clock))
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

        cur.execute("SELECT total_bytes FROM disk_total WHERE used_itemid = %s", (itemid_str,))
        total_row = cur.fetchone()
        if total_row is None:
            cur.close()
            conn.close()
            return False
        total_bytes = float(total_row[0])

        cleanup_date = detect_cleanups(df, total_bytes, 10.0)
        if cleanup_date is not None:
            window_start = cleanup_date - timedelta(days=1)
            window_end = cleanup_date + timedelta(days=1)
            window = df[(df['ds'] >= window_start) & (df['ds'] <= window_end)]
            if not window.empty:
                anomaly_row = window.loc[window['y'].idxmin()]
                df = df[df['ds'] != anomaly_row['ds']]

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

        future = m.make_future_dataframe(df, periods=DAYS_FORECAST)
        forecast = m.predict(future)

        cur.execute("DELETE FROM forecasts WHERE itemid = %s", (itemid_str,))
        run_id = str(datetime.now(timezone.utc).timestamp())
        forecast_run_at = datetime.now(timezone.utc)

        last_hist_date = df['ds'].max()
        future_forecast = forecast[forecast['ds'] > last_hist_date]

        for _, row in future_forecast.iterrows():
            yhat = row.get('yhat1')
            if pd.isna(yhat) or yhat is None:
                continue
            
            yhat_lower = row.get('yhat1_lower', yhat * 0.99)
            yhat_upper = row.get('yhat1_upper', yhat * 1.01)
            
            if pd.isna(yhat_lower):
                yhat_lower = yhat * 0.99
            if pd.isna(yhat_upper):
                yhat_upper = yhat * 1.01

            cur.execute("""
                INSERT INTO forecasts (
                    itemid, run_id, forecast_run_at, ds, yhat, yhat_lower, yhat_upper,
                    threshold_pct, fill_date_est
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                itemid_str,
                run_id,
                forecast_run_at,
                row['ds'].to_pydatetime(),
                float(yhat),
                float(yhat_lower),
                float(yhat_upper),
                threshold_pct,
                None
            ))

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
        SELECT itemid, total_itemid, COALESCE(alert_threshold_pct, 90.0)
        FROM prediction_targets 
        WHERE enabled = true
    """)
    targets = cur.fetchall()
    cur.close()
    conn.close()

    completed = 0
    max_workers = min(multiprocessing.cpu_count(), 8)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_itemid = {
            executor.submit(forecast_single_disk, itemid, total_itemid, threshold_pct, DB_CONFIG): (itemid, i)
            for i, (itemid, total_itemid, threshold_pct) in enumerate(targets, 1)
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

    print(f"\n🏁 Завершено: {completed}/{len(targets)} дисков")

if __name__ == "__main__":
    main()
