import urllib.request
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pickle
from multiprocessing import Pool, Manager
import time
from google.cloud import storage
import os

storage_client = storage.Client()

filename = 'site1.json'
bucket = storage_client.get_bucket("br_investing")
if filename:
    blob = bucket.get_blob(f'org_site/{filename}')
    datastore = json.loads(blob.download_as_string())


hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'}


def testando(pre_key, _key, dici):
    
    base = 'https://br.investing.com/'
    MAX = 10

    first_url = base + pre_key + '/' + _key
    print(f'Fetching: {first_url}')
    try:
        first_request = urllib.request.Request(first_url,headers=hdr)
        first_html = urllib.request.urlopen(first_request)
    except:
        print(f'Erro na primeira pagina - {first_url}')
        return 'None'
    try:    
        bs = BeautifulSoup(first_html, 'lxml')
        first = bs.find('div', class_='largeTitle')
        articles = first.find_all("article")
    except:
        print(f'Erro em pegar articles da primeira pagina - {first_url}')
    try:    
        next_page = bs.find('div', class_='sideDiv inlineblock text_align_lang_base_2')
        next_page = next_page.find('a', href=True)['href']
    except:
        next_page = None
        print(f'Erro em obter o link da proxima pagina')

    itera = 0
    while itera < MAX:
        print(f'Itera - {itera} of {MAX}')
        for idx, _article in enumerate(articles):
            try:
                _idd = _article['data-id']
            except:
                _idd = 'no_id'
            
            try:    
                _title = _article.find('a', class_='title')['title']
            except:
                _title = 'no_title'
            try:    
                _link = _article.find('a', class_='title')['href']
            except:
                _link = 'no_link'
            try:    
                _fonte = _article.find('span', class_='articleDetails').span.text
            except:
                _fonte = 'no_fonte'

            if _idd != 'no_id':
                if _idd not in dici.keys():
                    dici[_idd] = {'title': _title, 'link': _link, 'fonte': _fonte, 'present': [_key]}
                    
                else:
                    print(f'data_id duplicated: {_idd}')
                    dici[_idd]['present'].append(_key)
                                  
        if next_page != None:
            url_new = base + next_page
            try:
                request = urllib.request.Request(url_new,headers=hdr)
                html = urllib.request.urlopen(request)
            except:
                print(f'Erro na request da pagina')
                break
            try:          
                bs = BeautifulSoup(html, 'lxml')
                dc = bs.find('div', class_='largeTitle')
            except:
                print(f'Erro no BeatifulSoup')
                break
            try:          
                articles = dc.find_all("article")
            except:
                print(f'Erro em obter articles')
                break 
            try:          
                next_page = bs.find('div', class_='sideDiv inlineblock text_align_lang_base_2')
                next_page = next_page.find('a', href=True)['href']
                print(f'Next Page: {next_page}')
            except:
                print(f'Erro em obter o link da proxima pagina')
                next_page = None
        else:
            break

        itera += 1  

def metid2(dici, processes=4):

    base = 'https://br.investing.com'
    pool = Pool(processes=processes)
    pre_key = 'news'
    [pool.apply_async(testando, args=(pre_key, _key, dici)) for _key in datastore[base][pre_key]]
    pool.close()
    pool.join()
    print(f'Finalizando')

    
def funcao_marota(key, dici, idx, tam):
    if idx % 5 == 0:
        print(f'fecthing: {idx} of {tam}')

    base = 'https://br.investing.com'
          
    _link = dici[key]['link']  
    if 'http' in _link:
        return 'None'
    else:
        _full_link = base + _link
        try:
            request = urllib.request.Request(_full_link,headers=hdr)
            html = urllib.request.urlopen(request)
        except:
            print(f'Erro na request da pagina - {_full_link}')
            return 'None'
        try:          
            bs = BeautifulSoup(html, 'lxml')
            dc = bs.find('div', class_='wrapper')
            dc = dc.find('section', id='leftColumn')
        except:
            print(f'Erro no BeatifulSoup da pagina - {_full_link}')
            return 'None'
        try:
            _header = dc.find('h1', class_='articleHeader').text
        except:
            _header = 'no_header'
        try:
            _data = dc.find('div', class_='contentSectionDetails').span.text
        except:
            _data = 'no_data'
        _data_scrap = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        try:
            dc = dc.find('div', class_='WYSIWYG articlePage')
        except:
            print('erro em obter o texto - WYSIWYG articlePage')
            return 'None'
        try:
            _text = dc.find_all('p')
        except:
            print('erro em obter o texto - p')
            return 'None'
         
        _text_final_interm = []
        for text in _text:
            _text_final_interm.append(text.text)
        _text_final = "/n".join(_text_final_interm)
        
        return key, _header, _data, _data_scrap, _text_final

def metid(dici, processes=4):
    pool = Pool(processes=processes)           
    def aggregator(res): 
        if res != 'None':
            dici[res[0]]['header'] = res[1]
            dici[res[0]]['data_article'] = res[2]
            dici[res[0]]['data_scrap'] = res[3]
            dici[res[0]]['text'] = res[4]

    tam = len(dici.keys())
    [pool.apply_async(funcao_marota, args=(_key, dici, idx, tam), callback=aggregator) for idx, _key in enumerate(dici.keys())]
    pool.close()
    pool.join()
    print(f'Finalizando')

    return dici

def _save_file(dici_final):
    date_file = datetime.now().strftime("%d-%m-%Y-%H:%M")
    blob = bucket.blob(f'write_p/{date_file}.pickle')
    blob.upload_from_string(
        data=json.dumps(dici_final),
        content_type='application/json'
       )
    print('Saved')   

'''if __name__ == "__main__":
    manager = Manager()
    dici = manager.dict()

    inicio = time.asctime(time.localtime(time.time()))
    metid2(dici, processes=10)
    dici_t = dici.copy()
    dc = metid(dici_t, processes=20)
    _save_file(dc)
    fim = time.asctime(time.localtime(time.time()))
    print(f'Inicio: {inicio} - Fim: {fim}')'''

def main(request):
    manager = Manager()
    dici = manager.dict()

    inicio = time.asctime(time.localtime(time.time()))
    metid2(dici, processes=10)
    dici_t = dici.copy()
    dc = metid(dici_t, processes=20)
    _save_file(dc)
    fim = time.asctime(time.localtime(time.time()))
    print(f'Inicio: {inicio} - Fim: {fim}')
    return {'Sucesso': 'True'}    
    
    
