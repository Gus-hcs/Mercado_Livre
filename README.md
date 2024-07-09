# Mercado_Livre

Este projeto contém uma coleção de scripts em Python desenvolvidos para integrar e manipular dados relacionados a pedidos e remessas da API do Mercado Livre. Os scripts abrangem desde a obtenção de tokens de acesso até a coleta e atualização de informações de pedidos. Abaixo está uma breve descrição de cada script presente neste repositório.

# Scripts

 1. _get_tokens_meli.py Este script é responsável por obter tokens de acesso e atualização necessários para autenticar e realizar chamadas à API do Mercado Livre. Ele utiliza as credenciais do usuário e a API de autenticação do Mercado Livre para gerar e renovar tokens conforme necessário.

 2. Collection_Order_Stores.py Este script coleta informações de pedidos de várias lojas vendendo no Mercado Livre. Ele se conecta a um banco de dados SQL Server para buscar dados de pedidos e utiliza chamadas assíncronas para obter dados atualizados de pedidos e remessas diretamente da API do Mercado Livre.

 3. Insert_View_Not_Exist.py Este script verifica a existência de determinadas visualizações no banco de dados e, caso não existam, as cria. Ele garante que as visualizações necessárias para a análise de dados estejam presentes e atualizadas no banco de dados.

 4. Update_Information.py Este script é responsável por atualizar informações específicas de pedidos no banco de dados. Ele faz uso de dados coletados da API do Mercado Livre e assegura que o banco de dados local esteja sempre sincronizado com as informações mais recentes disponíveis na plataforma.

- Requisitos Python 3.8 ou superior Bibliotecas: requests, aiohttp, pandas, pyodbc Conexão com banco de dados SQL Server Credenciais de API do Mercado Livre Como Usar Configuração do Ambiente: Instale todas as bibliotecas necessárias listadas nos requisitos.
- Obtenção de Tokens: Execute o script _get_tokens_meli.py para obter e renovar tokens de acesso. Coleta de Dados: Utilize Collection_Order_Stores.py para buscar dados de pedidos e remessas. 
- Verificação de Visualizações: Execute Insert_View_Not_Exist.py para garantir que todas as visualizações necessárias estão presentes no banco de dados. 
- Atualização de Informações: Use Update_Information.py para manter as informações de pedidos atualizadas no banco de dados.
