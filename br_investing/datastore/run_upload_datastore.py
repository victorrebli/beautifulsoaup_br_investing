from google.cloud import datastore
import os
import pickle
import glob
import json
from google.cloud import storage
import time

ds_client = datastore.Client()

def get_keys(_site, dici):
    query = ds_client.query(kind=_site)
    results = list(query.fetch())
    list_keys = []
    for _result in results:
        if _result.key.name in list(dici.keys()):
            list_keys.append(_result.key.name)
    return list_keys


def write_register(_site, _id, _register):
    task_key = ds_client.key(_site, _id)
    # Prepares the new entity
    task = datastore.Entity(key=task_key, exclude_from_indexes=['text'])
    ds_client.put(task)
    task.update(_register)
    ds_client.put(task)


def rename_blob(storage_client, bucket_name, _file, write):
    """Renames a blob."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(_file)

    new_blob = bucket.rename_blob(blob, write + _file.split('/')[1])
    print("Blob {} has been renamed to {}".format(blob.name, new_blob.name))


def princ():
    client = storage.Client()
    #files = glob.glob('data/br_investing/write_p/*.pickle')
    write = 'write_f/'
    _site = 'br_investing'
    print(f'Site: {_site}')
    bucket = client.get_bucket(_site)
    blobs = bucket.list_blobs(prefix='write_p')
    files = []
    for blob in blobs:
        files.append(blob.name)
    files.pop(0)
    if len(files) == 0:
        print('Não existe arquivos para processar')
        return 
        
    for _file in files:
        print(f'Insert the file: {_file}')
        blob = bucket.get_blob(_file)
        dici_full = json.loads(blob.download_as_string())
        tam = len(dici_full)
        res_keys_allowed = get_keys(_site,dici_full)
        article_not_included = 0
        article_alread_database = 0
        for idx, (_id, _register) in enumerate(dici_full.items()):
            if idx % 50 == 0:
                print(f'{idx} of {tam}')
            if _id not in res_keys_allowed:
                if 'text' in _register:
                    write_register(_site, _id, _register)
                else:
                    article_not_included += 1
                    #print(f'article {_id} not included')
            else:
                article_alread_database += 1    
                #print(f'{_id} já está no database')

        print(f'{article_alread_database} de {tam} nao foram incluidos pois já estao no database')
        print(f'{article_not_included} de {tam} não foram incluidos por outros motivos')
        print(f'{(article_alread_database + article_not_included) - tam} de {tam} foram incluidos')
        try:
            print(f"Movendo o arquivo: {_file} para a {write + _file.split('/')[1]}")
            rename_blob(client, _site, _file, write)
        except:
            print(f"Erro em mover o arquivo {_file} para a {write + _file.split('/')[1]}")
            continue

def main(request):
    inicio = time.asctime(time.localtime(time.time()))
    princ()
    query = ds_client.query(kind='br_investing')
    results = list(query.fetch())
    tam = len(results)  
    print(f'Tamanho da base: {tam} registros')
    fim = time.asctime(time.localtime(time.time()))
    print(f'Inicio: {inicio} - Fim: {fim}')
    return {'Sucesso': 'True'}                    

