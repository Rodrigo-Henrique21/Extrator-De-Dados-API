# Definindo as  bibliotecas

import requests
import time
import pandas as pd
import json
import copy
import azure.storage.blob
import datetime
from azure.storage.blob import PublicAccess, ContainerClient, BlobServiceClient, BlobClient


# Variaveis Globais

global blob_texts
blob_texts = {}

connection_string = "SUA_CONNECTION_STRING"
container_name = "SEU_CONTAINER"

# Definindo EndPoint Da API
url = 'https://app.omie.com.br/api/v1/'

start_time = time.time()

# Lendo o secret e key fornecidos pela  API
keys = {
    "SUA_KEY":"SEU_SECRET",
    "SUA_KEY":"SEU_SECRET"
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



# Definindo as funções


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
#         container_client.upload_blob(blob_name, blob_data, overwrite = True)
        blob_client.upload_blob(blob_data, overwrite = True)
        print('Upload finalizado: {}'.format(blob_name))
    


# Realiza Download dos blob já normalizados e faz sua conversão para .csv

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



# Verifica se a conversão e seu dowloand foram feitos e armazenados na variavel global.

data_names = pd.DataFrame(list(blob_texts)).values

if blob_texts == {}:
    print('Não há nenhum dado para ser utilizado!')
else:
    print('[{}]:[INFO] : Nome da Coleção de Dados: '.format(datetime.datetime.utcnow()) + data_names)


# Realiza o upload dos arquivos baixados e convertidos 

upload_all_data(blob_texts)
