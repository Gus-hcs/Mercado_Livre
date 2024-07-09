import pyodbc
import requests

# Conexão com o banco de dados
server = '###'
database = '###'
username = '###'
password = '###'
conn_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_string)

# Criando o cursor para realizar consultas
cursor = conn.cursor()
query = "SELECT seller_id, refresh_token, client_secret, client_id FROM ###.dbo.meli_tokens"
cursor.execute(query)

# Percorrendo os resultados da consulta
for row in cursor.fetchall():
    seller_id, old_refresh_token, client_secret, client_id = row

    # Preparando os dados para a chamada da API
    url = 'https://api.mercadolibre.com/oauth/token'
    headers = {'accept': 'application/json', 'content-type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': old_refresh_token
    }

    # Fazendo a chamada da API
    response = requests.post(url, headers=headers, data=data)
    response_data = response.json()

    # Atualizando o access_token e refresh_token na tabela
    if 'access_token' in response_data and 'refresh_token' in response_data:
        new_access_token = response_data['access_token']
        new_refresh_token = response_data['refresh_token']
        update_query = f"UPDATE ###.dbo.meli_tokens SET access_token = ?, refresh_token = ? WHERE seller_id = ?"
        cursor.execute(update_query, (new_access_token, new_refresh_token, seller_id))
        conn.commit()
        print(f'Access and refresh tokens updated for seller_id: {seller_id}')
    else:
        print(f'Failed to update tokens for seller_id: {seller_id}, response: {response_data}')

# Fechando a conexão
cursor.close()
conn.close()
