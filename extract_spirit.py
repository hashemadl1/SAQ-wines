
import time
import requests
from selectorlib import Extractor
from bs4 import BeautifulSoup
import re
import ast
import urllib.request
import unicodedata
from bitstring import BitArray
import pickle
import pandas as pd
import math
headers = {
        'authority': 'www.saq.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US,en-CA;q=0.9,en;q=0.8',
    }

SAQ_START_LINK = "https://www.saq.com/en/products/spirit"
#SAQ_START_LINK = "https://www.saq.com/en/products/wine"

def get_info_new(mystr):
    info_dict = {}
    splits = re.split(r'<strong data-th=', mystr)
    for item in splits[1:]:
        tmp = item[0:item.index('</strong>')].replace('"', '').replace("'", "")
        two_parts = tmp.split('>')
        info_dict[unicodedata.normalize("NFKD",two_parts[0].strip())]= unicodedata.normalize("NFKD",two_parts[1].strip())

    return info_dict


def get_info2(mystr):
    idx1=mystr.index('{')
    idx2=mystr.index('}')
    sub_str = mystr[idx1:idx2+1]
    dic = ast.literal_eval(sub_str)
    return dic

def process_image(image_url, product_code):
    if image_url is not None:
        local_image_name = product_code+".png"

        if '?' in image_url:
            index_qm = image_url.index('?')
            clean_image_url = image_url[:index_qm]
        elif '.png' in image_url:
            index_qm = image_url.index('.png')
            clean_image_url = image_url[:(index_qm+4)]
        else:
            clean_image_url = image_url
        urllib.request.urlretrieve(clean_image_url, filename=local_image_name)
        return {'image_link':clean_image_url}

    return {'image_link': None}


def get_info_from_url(url):
    html = requests.get(url, headers=headers).text
    extract=Extractor.from_yaml_file('selector_saq.yml')
    fields = extract.extract(html)
    soup = BeautifulSoup(html, "html.parser")
    txt_contains_prod = ""
    for node in soup.find_all(text=lambda x: x and "productInfoObject" in x):
        if 'productInfoObject = {' in node:
            idx=node.index('productInfoObject = {')
            subs=node[idx:]
            idx = subs.index('}')
            subs= subs[:idx+1]
            txt_contains_prod= subs
            break
    dic1 = get_info_new (fields['prd_attributs'])
    dic2 = get_info2(txt_contains_prod)
    if 'SAQ code' in dic1:
        product_code = dic1['SAQ code']
    elif 'productSku' in dic2:
        product_code = dic2['productSku']
    else:
        print('Something is wrong!')
        product_code='unknown'

    dic_image = process_image(fields['image'],product_code)
    dic1.update(dic2)
    dic1.update(dic_image)
    other_links = get_all_links(html=html)
    return dic1 , other_links

def get_all_links (html):
    urls = re.findall(r'https://www.saq.com/en/[0-9]+', html)
    urls = list(set(urls))
    return urls

def save_to_file(dataset, visited_url):
    df = pd.DataFrame(data=dataset)
    df.to_csv('our_spirit_dataset.csv', encoding='utf-8-sig',index= False)
    #df.to_csv('our_wine_dataset.csv', encoding='utf-8-sig',index= False)
    with open('visited_urls', 'wb') as fp:
        pickle.dump(visited_url, fp)


def get_number_of_pages():
    url = SAQ_START_LINK
    html = requests.get(url, headers=headers).text
    extract = Extractor.from_yaml_file('selector_saq2.yml')
    fields = extract.extract(html)
    captured_txt = fields['nprds'].lower()
    no_products=0
    n_per_page=0
    nPages=0
    if 'results' in captured_txt:
        parts = captured_txt.split('of')
        no_products = int(parts[1].strip())
        new_parts = parts[0].replace('results', '').split('-')
        n_per_page = int(new_parts[1].strip()) - int(new_parts[0].strip()) + 1
        nPages = math.ceil(no_products // n_per_page)

    return {'nProducts':no_products, 'nPerPage':n_per_page, 'nPages':nPages}

visited_urls=[]
dataset=[]
counter=0
visited_urls_bits = BitArray(100000000)
n_faulty_pages=0
n_prds_found=0

page_info = get_number_of_pages()

for pid in range(page_info['nPages']):
    page_url = 'https://www.saq.com/en/products/spirit?p={}'.format(pid+1)
    #page_url = 'https://www.saq.com/en/products/wine?p={}'.format(pid+1)
    r = requests.get(page_url)
    if (r.status_code == 200)  and (r.text):
        html = r.text
        urls=get_all_links(html)

        for  url in urls:
            product_code = int(re.sub("[^0-9]", "", url))
            if visited_urls_bits[product_code] == 0:
                n_prds_found +=1
                visited_urls.append(url)
                visited_urls_bits[product_code] =1
                try:
                    dic_ret, other_links = get_info_from_url(url)
                    print(n_prds_found, dic_ret)
                    dataset.append(dic_ret)
                except:
                    print(url)
                    pass

                counter +=1
                if counter == 30:
                    save_to_file(dataset,visited_urls)
                    counter=0
                   
                time.sleep(2)
        
    else:
        print(page_url)
        print('Something was wrong in this page. Ignored!')
        n_faulty_pages +=1

    if n_faulty_pages==3:
        print('Something went wrong. Scrapping was cancelled!')
        break


save_to_file(dataset,visited_urls)


