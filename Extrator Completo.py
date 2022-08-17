# DEFINIÇÃO BIBLIOTECAS

import copy
import requests
import time
import csv
import pandas as pd
import azure.storage.blob
import json
import os
import datetime
import io
from os import walk
from pathlib import Path
from time import sleep
from azure.storage.blob import PublicAccess, ContainerClient, BlobServiceClient, BlobClient
from multiprocessing.pool import ThreadPool




# DEFINIÇÃO DAS VARIAVEIS GLOBAIS

global blob_texts
blob_texts = {}

connection_string = "SUA_CONNECTION_STRING"
container_name = "SEU_CONTAINER"
url = 'https://app.omie.com.br/api/v1/'

start_time = time.time()

# Lendo arquivos de configuração
keys = {
    "SUA_KEY":"SUA_SECRET",
    "SUA_KEY":"SUA_SECRET"
}

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

# Para a normalização, apenas coloque o nome da chave e uma lista de chaves 
# do objeto a ser normalizado

normalize_keys = {
    'dadosDRE': [ 'codigoDRE', 'descricaoDRE', 'naoExibirDRE', 'nivelDRE', 'sinalDRE', 'totalizaDRE' ],
    'cabecalho': [ 'dDtLanc', 'nCodCC', 'nValorLanc' ],
    'departamentos': [ 'cCodDep', 'nPerDep', 'nValDep' ],
    'detalhes': [ 'cCodCateg', 'cNumDoc', 'cObs', 'cTipo', 'nCodCliente', 'nCodProjeto' ],
    'diversos': [ 'cHrConc', 'cIdentLanc', 'cNatureza', 'cOrigem', 'cUsConc', 'dDtConc', 'nCodComprador', 'nCodVendedor' ],
    'info': [ 'cImpAPI', 'dAlt', 'dInc', 'hAlt', 'hInc', 'uAlt', 'uInc' ],
    'transferencia': [ 'nCodCCDestino' ],
    'resumo': [ 'cLiquidado', 'nDesconto', 'nJuros', 'nMulta', 'nValAberto', 'nValLiquido', 'nValPago' ]
}

# Use dessa forma:
# Se um arquivo com X nome (vai ser uma chave do objeto abaixo) tiver que normalizar 
# dados que tenham uma mesma chave acima, coloque o nome e a lista de chaves...

# EXEMPLO: o objeto "normalize_keys" tem a chave "detalhes", mas ela é diferente do que se precisa
# no arquivo "ListarMovimentos", por isso você irá colocar o nome do arquivo e como valor um objeto 
# de normalização como os usados acima. 
# Use o valor abaixo como base.
custom_normalize_keys = {
    'ListarMovimentos': {
        'detalhes': [ 'cCPFCNPJCliente', 'cCodCateg', 'cGrupo', 'cNatureza', 'cNumCtr', 'cNumOS',
                'cNumParcela', 'cNumTitulo', 'cOperacao', 'cOrigem', 'cRetIR', 'cStatus', 'cTipo', 'dDtEmissao', 
                'dDtPrevisao', 'dDtRegistro', 'dDtVenc', 'nCodCC', 'nCodCliente', 'nCodCtr', 'nCodNF', 'nCodOS',
                'nCodTitRepet', 'nCodTitulo', 'nValorIR', 'nValorTitulo' ] 
    }
}


# DEFINIÇÃO DAS FUNÇÕES


def create_obj(keys, value = ''):
    return { i: copy.copy(value) for i in keys }        

def normalize_level(data, principal_key, default_keys):
    if principal_key in data:
        if not isinstance(data[principal_key]['0'], (dict, list)):
            return None
        temp_data = create_obj(default_keys, {})
        for key, value in data[principal_key].items():
            if not isinstance(value, (dict, list)) or value == []:
                continue
            if value == {}:
                value = create_obj(default_keys)
            if isinstance(value, list):
                value = value[0]
            for dkey, dvalue in value.items():
                if dkey in default_keys:
                    temp_data[dkey][key] = dvalue
        del data[principal_key]    
        data.update(temp_data)
    return data

def normalize_keys_data(data, pre_normalize_arr = {}):
    data = json.loads(data.content_as_text())
    data = pd.json_normalize(data, max_level = 0).iloc[0].to_dict()
    
    for key, default_value in pre_normalize_arr.items():
        normalize_level(data, key, default_value) 
    
    for key, default_value in normalize_keys.items():
        normalize_level(data, key, default_value) 
    
    if data == {} or data == []:
        return None
    
    return pd.DataFrame(pd.json_normalize(data, max_level = 0).iloc[0].to_dict()).to_csv(index = False)

def normalize_blob_data(data, max_level = -1):
    return pd.json_normalize(json.loads(data.content_as_text()), max_level = max_level).iloc[0].to_json()

def download_and_contain_blob(blob_client, blob_name):
    print('[{}]:[INFO] : Blob name: {}'.format(datetime.datetime.utcnow(), blob_name))
    print("[{}]:[INFO] : Downloading {} ...".format(datetime.datetime.utcnow(), blob_name))
    blob_name = blob_name.split('.')[0] + '.csv'
    blob_data = blob_client.download_blob()
    
    pre_normalize = {}
    for normalize_name, normalize_data in custom_normalize_keys.items():
        if normalize_name in blob_name:
            pre_normalize.update(normalize_data)
    
    blob_text = normalize_keys_data(blob_data, pre_normalize)
    blob_texts.update({blob_name: blob_text})
    print("[{}]:[INFO] : download finished".format(datetime.datetime.utcnow()))

def download_blob(blob_client, blob_name):
    print('[{}]:[INFO] : Blob name: {}'.format(datetime.datetime.utcnow(), blob_name))
    print("[{}]:[INFO] : Downloading {} ...".format(datetime.datetime.utcnow(), blob_name))
    blob_data = blob_client.download_blob()
    print("[{}]:[INFO] : download finished".format(datetime.datetime.utcnow()))
    return blob_data
    
def upload_all_data(data, connection_string = connection_string, container_name = 'SEU_CONTAINER'):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print(container_client)
    print('Fazendo Upload arquivos...')

    for blob_name, blob_data in data.items():
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(blob_data, overwrite = True)
        print('Upload finalizado: {}'.format(blob_name))

# Usa todas as chaves passadas para conectar e extrair
for app_key, app_secret in keys.items():
    
    # Extrai de todas as rotas passadas
    for route, call in routes.items():
        print("Extracting from {} method...".format(call))

        # Nome para salvar arquivos com os dados coletados
        filename = '{}_{}.json'.format(app_key, call)  
        
        # Definição dos parametros dos relátorios
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
            
                 
        container_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = container_client.get_blob_client(container=container_name, blob = filename)
        
        output = pd.DataFrame(data).to_json()
        blob_client.upload_blob(output, blob_type="BlockBlob", overwrite = True)

# REALIZA DOWNLOAD DOS ARQUIVOS EM JSON 

connection_string = "SUA_CONNECTION_STRING"
container_name = "SEU_CONTAINER"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

blob_list = container_client.list_blobs()
for blob in blob_list:
    if blob.name.split('.')[1].lower() != 'json':
        continue
    blob_client = container_client.get_blob_client(blob.name)
    download_and_contain_blob(blob_client, blob.name)

blob_texts = { k: v for k, v in blob_texts.items() if v } 

print(blob_texts)

data_names = pd.DataFrame(list(blob_texts)).values


# VERIFICA SE A VARIAVEL POSSUI DADOS 
if blob_texts == {}:
    print('Não há nenhum dado para ser utilizado!')
else:
    print('[{}]:[INFO] : Nome da Coleção de Dados: '.format(datetime.datetime.utcnow()) + data_names)

upload_all_data(blob_texts)

# JUNTA TODOS CSV'S
connection_string = "DefaultEndpointsProtocol=https;AccountName=staengdados;AccountKey=KtfGJ/u3NWxqsFBksx2gR8hRVAcpV0lsVr9liYwsXJoTx68DIa2KtFVobhO6Ob3bmo8PcobxzNYk+AStltMUjA==;EndpointSuffix=core.windows.net"
container_name = "clean"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs()
tmp_data_frames = {}
for blob in blob_list:
    if blob.name.split('.')[1] != 'csv':
        continue
    if len(blob.name.split('_')) != 2:
        continue
    blob_client = container_client.get_blob_client(blob.name)
    data = download_blob(blob_client, blob.name)
    try:
        csv_string = data.content_as_text()
    except:
        csv_string = data.content_as_text(encoding='latin1')
    csvIO = io.StringIO(csv_string)
    csv_df = pd.read_csv(csvIO, sep=",")
    name_type_csv = blob.name.split('_')[-1]
    if name_type_csv in tmp_data_frames:
        tmp_data_frames.update({
            name_type_csv: pd.concat([tmp_data_frames[name_type_csv], csv_df])
        })
    else:
        tmp_data_frames.update({name_type_csv: csv_df})
tmp_data_frames = { k: v.to_csv(index=False) for k, v in tmp_data_frames.items() } 

# FAZ O UPLOAD DOS CSV'S JUNTOS
connection_string = "SUA_CONNECTION_STRING"
container_name = "SEU_CONTAINER"

# REALIZA UPLOAD DOS DADOS 
upload_all_data(tmp_data_frames, connection_string, container_name)


# JUNTA TODOS CSV'S
connection_string = "SUA_CONNECTION_STRING"
container_name = "SEU_CONTAINER"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs()
tmp_data_frames = {}
for blob in blob_list:
    if blob.name.split('.')[1] != 'csv':
        continue
    if len(blob.name.split('_')) != 2:
        continue
    blob_client = container_client.get_blob_client(blob.name)
    data = download_blob(blob_client, blob.name)
    try:
        csv_string = data.content_as_text()
    except:
        csv_string = data.content_as_text(encoding='latin1')
    csvIO = io.StringIO(csv_string)
    csv_df = pd.read_csv(csvIO, sep=",")
    #csv_df.drop(columns = csv_df.columns[0], axis = 1, inplace = True)
    name_type_csv = blob.name.split('_')[0] +'_'+ 'Segue'+'.csv'
    if name_type_csv in tmp_data_frames:
        tmp_data_frames.update({
            name_type_csv: pd.concat([tmp_data_frames[name_type_csv], csv_df])
        })
    else:
        tmp_data_frames.update({name_type_csv: csv_df})
tmp_data_frames = { k: v.to_csv(index=False) for k, v in tmp_data_frames.items() } 


# FAZ O UPLOAD DOS CSV'S JUNTOS
connection_string = "SUA_CONNECTION_STRING"
container_name = "SEU_CONTAINER"

upload_all_data(tmp_data_frames, connection_string, container_name)