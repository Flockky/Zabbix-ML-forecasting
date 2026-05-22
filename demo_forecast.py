# validation_forecast.py
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
import traceback

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

# Настройки
DAYS_BACK = 30          # Откатываемся на 30 дней назад
VALIDATION_DAYS = 30    # Прогнозируем на 30 дней вперед
HISTORY_DAYS = 365      # Используем год истории

def detect_cleanups(df, total_bytes, drop_pct_threshold=10.0):
    df = df.sort_values('ds').reset_index(drop=True)
    if len(df) < 2:
        return None
    df['prev'] = df['y'].shift(1)
    df['drop_bytes'] = df['prev'] - df['y']
    df['drop_pct'] = (df['drop_bytes'] / total_bytes) * 100
    cleanups = df[df['drop_pct'] >= drop_pct_threshold]
    return cleanups['ds'].max() if not cleanups.empty else None

def validate_single_disk(itemid, db_config):
    """Валидация с сохранением полного прогноза по дням"""
    conn = None
    cur = None
    try:
        itemid_str = str(itemid)
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # 1. Определяем cutoff дату (30 дней назад)
        cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
        cutoff_clock = int(cutoff_date.timestamp())
        
        # 2. Загружаем исторические данные ДО cutoff даты
        historical_cutoff = int(datetime.now().timestamp()) - HISTORY_DAYS * 86400
        cur.execute("""
            SELECT clock, value_avg 
            FROM zabbix_trends 
            WHERE itemid = %s AND value_avg IS NOT NULL AND clock >= %s AND clock <= %s
            ORDER BY clock
        """, (itemid_str, historical_cutoff, cutoff_clock))
        used_data = cur.fetchall()
        
        if not used_data or len(used_data) < 10: # Увеличил мин. порог до 10 точек
            cur.close()
            conn.close()
            return False, None, "Недостаточно данных"

        df = pd.DataFrame(used_data, columns=['ds', 'y'])
        df['ds'] = pd.to_datetime(df['ds'], unit='s')
        df = df.dropna().sort_values('ds').reset_index(drop=True)

        if df['y'].nunique() < 2:
            cur.close()
            conn.close()
            return False, None, "Мало уникальных значений"

        # 3. Получение total_bytes
        cur.execute("SELECT total_bytes FROM disk_total WHERE used_itemid = %s", (itemid_str,))
        total_row = cur.fetchone()
        if total_row is None:
            cur.close()
            conn.close()
            return False, None, "Нет total_bytes"
        total_bytes = float(total_row[0])

        # 4. Обнаружение очисток
        cleanup_date = detect_cleanups(df, total_bytes, 10.0)
        if cleanup_date is not None:
            window_start = cleanup_date - timedelta(days=1)
            window_end = cleanup_date + timedelta(days=1)
            window = df[(df['ds'] >= window_start) & (df['ds'] <= window_end)]
            if not window.empty:
                anomaly_row = window.loc[window['y'].idxmin()]
                df = df[df['ds'] != anomaly_row['ds']]

        # 5. Обучение модели
        m = NeuralProphet(
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
            growth='linear',
            epochs=30,
            batch_size=64,
            learning_rate=1.0,
            quantiles=[0.1, 0.9]
        )
        m.fit(df, freq='D')

        # 6. Прогноз на VALIDATION_DAYS дней
        future = m.make_future_dataframe(df, periods=VALIDATION_DAYS)
        forecast = m.predict(future)

        # 7. Проверка роста диска (ГЛАВНОЕ ИЗМЕНЕНИЕ)
        if len(df) >= 2:
            total_growth = df['y'].iloc[-1] - df['y'].iloc[0]
            # Если рост <= 0, диск не заполняется. Помечаем как невалидный.
            if total_growth <= 0:
                is_invalid = True
                avg_daily_growth = 1.0 # Заглушка, чтобы не делить на 0
                validation_error = 0.0 # Ошибка 0, но флаг поднимет is_invalid
            else:
                is_invalid = False
                avg_daily_growth = total_growth / len(df)
        else:
            is_invalid = True
            avg_daily_growth = 1.0
            validation_error = 0.0

        # 8. Сохранение ПОЛНОГО прогноза в demo_forecasts
        run_id = str(datetime.now(timezone.utc).timestamp())
        cur.execute("DELETE FROM demo_forecasts WHERE itemid = %s", (itemid_str,))
        
        # Проходим по всем строкам прогноза (future)
        # Нам нужно сопоставить прогноз с реальными данными, если они есть
        for idx, row in forecast.iterrows():
            # Пропускаем исторические части (где есть y), берем только будущее
            # В NeuralProphet forecast содержит и историю, и будущее. 
            # Будущее начинается после последней даты df
            last_history_date = df['ds'].max()
            if row['ds'] <= last_history_date:
                continue
            
            # Получаем реальное значение на эту дату (если оно уже наступило)
            real_val = None
            if row['ds'].to_pydatetime() < datetime.now():
                clock_ts = int(row['ds'].timestamp())
                cur.execute("""
                    SELECT value_avg FROM zabbix_trends 
                    WHERE itemid = %s AND clock <= %s 
                    ORDER BY clock DESC LIMIT 1
                """, (itemid_str, clock_ts))
                res = cur.fetchone()
                if res:
                    real_val = float(res[0])

            # Вставляем строку
            # Важно: проверь имя колонки yhat! Обычно это 'yhat1' при квантилях, или 'yhat'
            yhat_col = 'yhat1' if 'yhat1' in forecast.columns else 'yhat'
            
            cur.execute("""
                INSERT INTO demo_forecasts (itemid, ds, yhat, actual_y, run_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                itemid_str,
                row['ds'].to_pydatetime(),
                float(row[yhat_col]) if pd.notna(row.get(yhat_col)) else None,
                real_val,
                run_id
            ))

        # 9. Расчет итоговой ошибки (берем последнюю точку прогноза)
        last_forecast_row = forecast.iloc[-1]
        yhat_col = 'yhat1' if 'yhat1' in forecast.columns else 'yhat'
        forecast_value = float(last_forecast_row[yhat_col]) if pd.notna(last_forecast_row.get(yhat_col)) else 0.0
        
        # Реальное значение на конец периода (для финальной метрики)
        validation_end_date = cutoff_date + timedelta(days=VALIDATION_DAYS)
        validation_end_clock = int(validation_end_date.timestamp())
        
        cur.execute("""
            SELECT value_avg FROM zabbix_trends 
            WHERE itemid = %s AND clock <= %s 
            ORDER BY clock DESC LIMIT 1
        """, (itemid_str, validation_end_clock))
        actual_result = cur.fetchone()
        
        if actual_result and not is_invalid:
            actual_value = float(actual_result[0])
            error_bytes = abs(forecast_value - actual_value)
            validation_error = error_bytes / avg_daily_growth
            
            # Фильтр аномально больших ошибок (защита от выбросов)
            if validation_error > 3650: 
                validation_error = 3650
        else:
            # Если реальных данных нет или диск невалидный
            if is_invalid:
                validation_error = 0.0 # Специальное значение
            else:
                validation_error = None # Нет данных для проверки

        # 10. Сохранение результата в validation_results
        # Добавили поле is_invalid_growth!
        cur.execute("""
            INSERT INTO validation_results 
            (itemid, error_days, disk_name, validated_at, is_invalid_growth)
            VALUES (%s, %s, (SELECT name FROM prediction_targets WHERE itemid = %s), NOW(), %s)
        """, (itemid_str, float(validation_error) if validation_error is not None else 0.0, itemid_str, is_invalid))

        conn.commit()
        cur.close()
        conn.close()
        
        status = "INVALID (No Growth)" if is_invalid else f"OK (Error: {validation_error:.1f}d)"
        return True, validation_error, status

    except Exception as e:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except: pass
        return False, None, str(e)

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("🧹 Очистка старых данных валидации...")
    cur.execute("DELETE FROM demo_forecasts")
    cur.execute("DELETE FROM validation_results")
    conn.commit()
    print("✅ Старые данные удалены")
    
    cur.execute("SELECT itemid FROM prediction_targets WHERE enabled = true")
    targets = cur.fetchall()
    cur.close()
    conn.close()

    if not targets:
        print("⚠️ Нет активных дисков")
        return

    completed = 0
    errors_list = []
    invalid_count = 0
    max_workers = min(multiprocessing.cpu_count(), 8)
    
    print(f"🚀 Запуск валидации для {len(targets)} дисков...")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_itemid = {
            executor.submit(validate_single_disk, itemid, DB_CONFIG): (itemid, i)
            for i, (itemid,) in enumerate(targets, 1)
        }
        
        for future in as_completed(future_to_itemid):
            itemid, i = future_to_itemid[future]
            try:
                success, error, status_msg = future.result(timeout=300)
                if success:
                    completed += 1
                    if "INVALID" in status_msg:
                        invalid_count += 1
                    elif error is not None and error > 0:
                        errors_list.append(error)
                    
                    print(f"   ✅ [{i}/{len(targets)}] {itemid} — {status_msg}")
                else:
                    print(f"   ⚠️  [{i}/{len(targets)}] {itemid} — Пропущен ({status_msg})")
            except Exception as e:
                print(f"   ❌ [{i}/{len(targets)}] {itemid} — Crash: {traceback.format_exc()}")

    # Статистика ТОЛЬКО по валидным дискам с ростом
    valid_errors = [e for e in errors_list if e <= 365]
    
    print(f"\n📊 Итоги валидации:")
    print(f"   Всего дисков: {len(targets)}")
    print(f"   Успешно: {completed}")
    print(f"   ❌ Отклонено (нет роста/ошибки): {len(targets) - completed + invalid_count}")
    print(f"   📉 Дисков без роста (игнорируются в метрике): {invalid_count}")
    
    if valid_errors:
        avg_error = sum(valid_errors) / len(valid_errors)
        median_error = sorted(valid_errors)[len(valid_errors) // 2]
        p90_idx = int(len(valid_errors) * 0.9)
        p90_error = sorted(valid_errors)[p90_idx] if p90_idx < len(valid_errors) else valid_errors[-1]
        
        print(f"\n🏆 Метрики качества (только растущие диски):")
        print(f"   Средняя ошибка: {avg_error:.1f} дней")
        print(f"   Медиана: {median_error:.1f} дней")
        print(f"   90-й перцентиль: {p90_error:.1f} дней")
    else:
        print("\n⚠️ Недостаточно данных для расчета метрик качества.")

if __name__ == "__main__":
    main()
