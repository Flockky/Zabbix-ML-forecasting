# discover_mountpoints.py
import requests
import psycopg2
import re
from datetime import datetime
import os
requests.packages.urllib3.disable_warnings()

# === Настройки Zabbix ===
ZABBIX_URL = os.environ.get("ZABBIX_URL")
API_TOKEN = os.environ.get("ZABBIX_API_TOKEN")

# === Настройки PostgreSQL ===
DB_CONFIG = {
    "dbname": os.environ.get("DB_DBNAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": "5432"
}

def get_zabbix_data(method, params):
    headers = {"Content-Type": "application/json-rpc", "Authorization": f"Bearer {API_TOKEN}"}
    data = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    }
    resp = requests.post(ZABBIX_URL.strip(), headers=headers, json=data, verify=False)
    if resp.status_code != 200 or 'error' in resp.json():
        raise Exception(f"Zabbix error: {resp.json().get('error', 'Unknown')}")
    return resp.json()['result']

def main():
    print("🔍 Получение списка активных хостов...")
    hosts = get_zabbix_data("host.get", {
        "output": ["hostid", "host"],
        "filter": {"status": "0"}  # только активные
    })
    print(f"✅ Найдено {len(hosts)} активных хостов")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    total_pairs = 0

    for host in hosts:
        host_id = host['hostid']
        host_name = host['host']
        print(f"\n🖥️  Обработка хоста: {host_name}")

        # Получаем все элементы данных типа vfs.fs.size
        items = get_zabbix_data("item.get", {
            "output": ["itemid", "name", "key_"],
            "hostids": [host_id],
            "search": {"key_": "vfs.fs.size"},
            "searchByAny": True,
            "filter": {"state": "0", "status": "0"}  # enabled & supported
        })

        # Группируем по mount point
        used_items = {}
        total_items = {}

        for item in items:
            key = item['key_']
            if 'vfs.fs.size' in key:
                try:
                    # Извлекаем путь и тип из ключа: vfs.fs.size[/pgdata,used]
                    path_type = key.split('[')[1].rstrip(']')
                    path, fs_type = path_type.rsplit(',', 1)
                    path = path.strip()
                    fs_type = fs_type.strip()

                    if fs_type == 'used':
                        used_items[path] = item['itemid']
                    elif fs_type == 'total':
                        total_items[path] = item['itemid']
                except Exception as e:
                    print(f"   ⚠️  Ошибка парсинга ключа '{key}': {e}")
                    continue

        # Находим пары и вставляем/обновляем в БД
        for mount_point in used_items:
            if mount_point in total_items:
                used_id = used_items[mount_point]
                total_id = total_items[mount_point]

                # Единый порог для всех
                threshold = 90.0

                try:
                    cur.execute("""
                        INSERT INTO prediction_targets 
                        (itemid, total_itemid, host, name, alert_threshold_pct, enabled, last_discovered_at)
                        VALUES (%s, %s, %s, %s, %s, true, NOW())
                        ON CONFLICT (itemid) DO UPDATE SET
                            total_itemid = EXCLUDED.total_itemid,
                            host = EXCLUDED.host,
                            name = EXCLUDED.name,
                            alert_threshold_pct = EXCLUDED.alert_threshold_pct,
                            enabled = EXCLUDED.enabled,
                            last_discovered_at = NOW()
                    """, (used_id, total_id, host_name, mount_point, threshold))
                    total_pairs += 1
                    print(f"   ✅ {mount_point}: used={used_id}, total={total_id}")
                except Exception as e:
                    print(f"   ❌ Ошибка вставки {mount_point}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n🎉 Обнаружено и сохранено {total_pairs} пар дисков!")

if __name__ == "__main__":
    main()
