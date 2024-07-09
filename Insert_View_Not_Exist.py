import aiohttp
import asyncio
import pyodbc
import requests
from datetime import datetime

MAX_RETRIES = 3
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

def conectar_sql_server():
    connection_string = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    try:
        conn = pyodbc.connect(connection_string)
        print("Conexão bem sucedida!")
        return conn
    except Exception as e:
        print("Erro ao conectar ao SQL Server:", e) 
        return None

def buscar_missing_orders(conn):
    cursor = conn.cursor()
    query = "SELECT order_id, seller_id FROM ###.dbo.missing_orders"
    cursor.execute(query)
    orders = cursor.fetchall()
    cursor.close()
    return orders

def inserir_order(conn, order_id, seller_id):
    cursor = conn.cursor()
    query = """
    IF NOT EXISTS (SELECT 1 FROM meli_orders WHERE order_id = ?)
    BEGIN
        INSERT INTO meli_orders (order_id, seller_id)
        VALUES (?, ?)
    END
    """
    cursor.execute(query, (order_id, order_id, seller_id))
    conn.commit()
    cursor.close()

def atualizar_dados_order(conn, order_id, marketplace_fee, list_cost, seller_id, date_created, invoice_key, invoice_series, invoice_number, cost, sale_fee, updated):
    cursor = conn.cursor()
    query = """
    UPDATE meli_orders
    SET marketplace_fee = ?, list_cost = ?, seller_id = ?, date_created = ?, invoice_key = ?, invoice_series = ?, invoice_number = ?, cost = ?, sale_fee = ?, updated = ?
    WHERE order_id = ?
    """
    cursor.execute(query, (marketplace_fee, list_cost, seller_id, date_created, invoice_key, invoice_series, invoice_number, cost, sale_fee, updated, order_id))
    conn.commit()
    cursor.close()
    print(f"order_id {order_id} atualizado.")

async def obter_dados_order(session, token, order_id, retry=0):
    url = f'https://api.mercadolibre.com/orders/{order_id}'
    headers = {
        'Authorization': f'Bearer {token}'
    }
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200 or response.status == 206:
                data = await response.json()
                marketplace_fee = sum(payment.get('marketplace_fee', 0) for payment in data.get('payments', []))
                sale_fee = sum(payment.get('sale_fee', 0) for payment in data.get('order_items', []))
                shipment_id = data['shipping']['id']
                list_cost, cost = await obter_dados_shipment(session, token, shipment_id)
                date_created = data['date_created']
                return marketplace_fee, list_cost, date_created, cost, sale_fee
            else:
                print(f'Falha ao obter dados para order_id {order_id}:', response.status)
                return None, None, None, None, None
    except aiohttp.ClientConnectorError as e:
        if retry < MAX_RETRIES:
            print(f"Tentativa de reconexão {retry+1} para order_id {order_id}.")
            await asyncio.sleep(1)  
            return await obter_dados_order(session, token, order_id, retry=retry+1)
        else:
            print(f"Atenção: Tentativas esgotadas para order_id {order_id}.")
            return None, None, None, None, None

async def obter_dados_shipment(session, token, shipment_id):
    url = f'https://api.mercadolibre.com/shipments/{shipment_id}'
    headers = {'Authorization': f'Bearer {token}'}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                shipping_option = data.get("shipping_option", {})
                list_cost = shipping_option.get("list_cost")
                cost = shipping_option.get("cost")
                return list_cost, cost
            else:
                print(f'Falha ao obter dados de remessa para shipment_id {shipment_id}:', response.status)
                return None, None
    except Exception as e:
        print("Erro ao obter dados de remessa:", e)
        return None, None

async def obter_dados_invoice(session, token, order_id, seller_id, retry=0):
    url = f'https://api.mercadolibre.com/users/{seller_id}/invoices/orders/{order_id}'
    headers = {
        'Authorization': f'Bearer {token}'
    }
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200 or response.status == 206:
                data = await response.json()
                invoice_series = data['invoice_series']
                invoice_number = data['invoice_number']
                invoice_key = data.get('attributes', {}).get('invoice_key')
                return invoice_key, invoice_series, invoice_number

            else:
                print(f'Falha ao obter dados para order_id {order_id}:', response.status)
                return None, None, None
    except aiohttp.ClientConnectorError as e:
        if retry < MAX_RETRIES:
            print(f"Tentativa de reconexão {retry+1} para order_id {order_id}.")
            await asyncio.sleep(1)  
            return await obter_dados_invoice(session, token, order_id, seller_id, retry=retry+1)
        else:
            print(f"Atenção: Tentativas esgotadas para order_id {order_id}.")
            return None, None, None

async def processar_vendedor(conn, seller, token):
    orders = buscar_missing_orders(conn)
    async with aiohttp.ClientSession() as session:
        for order_id, seller_id in orders:
            if seller_id == seller["seller_id"]:
                inserir_order(conn, order_id, seller_id)
                marketplace_fee, list_cost, date_created, cost, sale_fee = await obter_dados_order(session, token, order_id)
                invoice_key, invoice_series, invoice_number = await obter_dados_invoice(session, token, order_id, seller_id)
                if all([marketplace_fee, list_cost, date_created, cost, sale_fee, invoice_key, invoice_series, invoice_number]):
                    updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    atualizar_dados_order(conn, order_id, marketplace_fee, list_cost, seller_id, date_created, invoice_key, invoice_series, invoice_number, cost, sale_fee, updated)

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

async def main():
    conn = conectar_sql_server()
    if conn:
        for seller in sellers:
            token = get_access_token(seller["auth_data"])
            if token:
                await processar_vendedor(conn, seller, token)
        conn.close()

if __name__ == '__main__':
    asyncio.run(main())
