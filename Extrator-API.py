# Definindo as  bibliotecas

import requests
import time
import pandas as pd
import azure.storage.blob
import datetime
from azure.storage.blob import PublicAccess, ContainerClient, BlobServiceClient, BlobClient


# Variaveis globais

global blob_texts
blob_texts = {}

connection_string = "SUA_CONNECTION_STRING"
container_name = "NOME_DO_SEU_CONTAINER"


# Definindo EndPoint Da API

url = 'https://app.omie.com.br/api/v1/'

start_time = time.time()

# Lendo o secret e key fornecidos pela API
keys = {
    "SUA_KEY":"SEU_SECRET",
    "SUA_KEY":"SEU_SECRET"
}


# Definindo os relatórios da API 
routes = {
    "/geral/categorias/":"ListarCategorias",
    "/geral/departamentos/":"ListarDepatartamentos",
    "/financas/contacorrentelancamentos/":"ListarLancCC",
    "/geral/contacorrente/":"ListarContasCorrentes",
    "/geral/clientes/":"ListarClientesResumido",
    "/financas/mf/":"ListarMovimentos"
}

# Parâmetros padrão para requisição
headers = {"Content-type: application/json","encoding='utf-8'"}


# Usa todas as chaves passadas para conectar e extrair
for app_key, app_secret in keys.items():
    
    # Extrai de todas as rotas passadas
    for route, call in routes.items():
        print("Extracting from {} method...".format(call))

        # Nome para salvar arquivos com os dados coletados
        filename = '{}_{}.json'.format(app_key[-4:], call)  
        
        # Define os parametros de cada relatório
        if "ListarLancCC" in filename:
                params = {
                    "call": "{}".format(call),
                    "app_key": "{}".format(app_key),
                    "app_secret": "{}".format(app_secret),
                    "param": [
                        {
                            "nPagina": 1,
                            "nRegPorPagina": 50
                        }
                    ]
                }
        elif "ListarContasCorrentes" in filename:
            params = {
                "call": "{}".format(call),
                "app_key": "{}".format(app_key),
                "app_secret": "{}".format(app_secret),
                "param": [
                    {
                    "pagina": 1,
                    "registros_por_pagina": 100,
                    "apenas_importado_api": "N"
                    }
                ]
            }
        elif "ListarMovimentos" in filename:
            params = {
                "call": "{}".format(call),
                "app_key": "{}".format(app_key),
                "app_secret": "{}".format(app_secret),
                "param": [
                    {
                      "nPagina": 1,
                      "nRegPorPagina": 500
                    }
                ]
            }
        else:
            params = {
                "call": "{}".format(call),
                "app_key": "{}".format(app_key),
                "app_secret": "{}".format(app_secret),
                "param": [
                    {
                        "pagina": 1,
                        "registros_por_pagina": 100,
                        "apenas_importado_api": "N"
                    }
                ]
            }
        data = []
        page = 1

        # Busca dados de todas as páginas
        while True:
            params['page'] = page

            # Formata o url para pegar dados da página atual
            url_api = '{}{}'.format(url, route, str(params).replace('\'', "\""))

            # Fazendo a request pra página
            response = requests.post(url_api,json=params)

            if response.status_code == 200:
                content = response.json()[list(response.json().keys())[-1]]
                data += list(content)
                page += 1
            else:
                print('Found {} pages from {} route!!'.format(page, call))
            break
            
                 
        # Realiza o upload dos arquivos para container do azure     
        container_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = container_client.get_blob_client(container=container_name, blob = filename)
        
        output = pd.DataFrame(data).to_json()
        blob_client.upload_blob(output, blob_type="BlockBlob", overwrite = True)

