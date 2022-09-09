# DEFININDO AS BIBLIOTECAS

import pandas as pd
import azure.storage.blob
import json
import os
import copy
import datetime
import io
from azure.storage.blob import PublicAccess, ContainerClient, BlobServiceClient, BlobClient

# DEFININDO VARIAVEIS GLOBAIS

connection_string = "SUA_CONNECTION_STRING"
container_name = "NOME_DO_SEU_CONTAINER"

# DEFININDO AS FUNÇÕES

def create_obj(keys, value = ''):
    return { i: copy.copy(value) for i in keys }     

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


# JUNTA TODOS CSV'S

# JUNTA ARQUIVO 

    connection_string = ""
    container_name = ""
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    tmp_data_frames = {}

    def save_excel(excel_name, excel_bytes):
        container_name = "curated"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(excel_name)
        blob_client.upload_blob(excel_bytes, overwrite = True)

    def create_excel_bytes(df):
        excel_bytes = io.BytesIO()
        writer = pd.ExcelWriter(excel_bytes, engine='xlsxwriter')
        df.to_excel(writer, sheet_name = 'data', index=False)
        writer.save()
        excel_bytes.seek(0)
        return excel_bytes

    for blob in blob_list:
        print(blob.name)
        #continue
        if blob.name.split('.')[1] != 'csv':
            continue
        if blob.name == '':
            continue 
        if blob.name == '':
            continue 
        if blob.name == '':
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
        csv_df = pd.read_csv(csvIO, sep=";")
        name_type_csv = blob.name.split('_')[0] +'_'+ 'Segue'+'.xlsx'
        if name_type_csv in tmp_data_frames:
            tmp_data_frames.update({
                name_type_csv: pd.concat([tmp_data_frames[name_type_csv], csv_df])
            })
        else:
            tmp_data_frames.update({name_type_csv: csv_df})

    for excel_name, excel_df in tmp_data_frames.items():
        excel_bytes = create_excel_bytes(excel_df)
        save_excel(excel_name, excel_bytes)


# FAZ O UPLOAD DOS CSV'S JUNTOS
connection_string = "SEU_CONNECTION_STRING"
container_name = "SEU_CONTAINER"

upload_all_data(tmp_data_frames, connection_string, container_name)
