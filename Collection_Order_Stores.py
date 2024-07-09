import aiohttp
import asyncio
import pyodbc
import requests
from datetime import datetime, timedelta, timezone

offset = timezone(timedelta(hours=-4))
now = datetime.now(offset)
date_from = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
date_to = now.replace(hour=23, minute=59, second=59, microsecond=0)

date_from_str = '2024-07-08T00:00:00.000-04:00'
date_to_str = '2024-07-09T23:59:59.999-04:00'

base_url = "https://api.mercadolibre.com/"

server = '###'
database = '###'
username = '###'
password = '###'

sellers = [
    {
        "seller_id": "###",
        "store_name": "###",
        "auth_data": {
            "grant_type": "refresh_token",
            "client_id": "###",
            "client_secret": "###",
            "refresh_token": "###"
        }
    },
        {
        "seller_id": "###",
        "store_name": "###",
        "auth_data": {
            "grant_type": "refresh_token",
            "client_id": "###",
            "client_secret": "###",
            "refresh_token": "###"
        }
    }
]

def get_access_token(auth_data):
    endpoint = "oauth/token"
    url = base_url + endpoint
    try:
        response = requests.post(url, data=auth_data)
        data = response.json()
        access_token = data.get("access_token", "")
        return access_token
    except Exception as e:
        print(f"Erro na requisição à API (get_access_token): {e}")
        return ""

async def get_all_order_ids(seller_id, headers):
    all_order_ids = []
    page_size = 51  
    try:
        endpoint = f"orders/search?seller={seller_id}&order.date_created.from={date_from_str}&order.date_created.to={date_to_str}"
        url = base_url + endpoint
        response = requests.get(url, headers=headers)
        total_records = response.json().get("paging", {}).get("total", 0)

        for offset in range(0, total_records, page_size):
            endpoint = f"orders/search?seller={seller_id}&offset={offset}&limit={page_size}&order.date_created.from={date_from_str}&order.date_created.to={date_to_str}"
            url = base_url + endpoint
            response = requests.get(url, headers=headers)
            data = response.json()
            current_order_ids = [order.get("id") for order in data.get("results", [])]
            all_order_ids.extend(current_order_ids)

        return all_order_ids
    except Exception as e:
        print(f"Erro na requisição à API (get_all_order_ids) para seller {seller_id}: {e}")
        return []

def order_id_exists(order_id, cursor):
    cursor.execute("SELECT COUNT(*) FROM ###.orders WHERE order_id = ?", (order_id,))
    count = cursor.fetchone()[0]
    return count > 0

def insert_order_ids_to_sql_server(order_ids, seller_id, store_name):
    try:
        connection = pyodbc.connect(
            f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
        )
        cursor = connection.cursor()
        
        for order_id in order_ids:
            if not order_id_exists(order_id, cursor):
                cursor.execute("INSERT INTO ###.orders (order_id, seller_id, store_name) VALUES (?, ?, ?)", (order_id, seller_id, store_name))
                print(f"Order ID {order_id} inserido no banco de dados para seller {seller_id}.")
            else:
                print(f"Order ID {order_id} já existe no banco de dados para seller {seller_id}. Ignorando inserção.")
        
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Erro ao inserir dados no banco de dados: {e}")

async def main():
    for seller in sellers:
        seller_id = seller["seller_id"]
        store_name = seller["store_name"]
        auth_data = seller["auth_data"]
        access_token = get_access_token(auth_data)
        if not access_token:
            print(f"Erro ao obter token para seller {seller_id}. Pulando para o próximo.")
            continue

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        all_order_ids = await get_all_order_ids(seller_id, headers)
        insert_order_ids_to_sql_server(all_order_ids, seller_id, store_name)

if __name__ == "__main__":
    asyncio.run(main())