#!/usr/bin/env python3

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import urllib.request
import pandas as pd
import re
import threading
import logging
import time
import random
import glob
import os

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')

class B3():
    def __init__(self, output_file, base_url, url, col_name):
        self.ua = UserAgent() 
        self.proxies = []

        self.sleep_time = 60
        self.output_file = output_file
        self.output_dir = output_file + '/'

        self.base_url = base_url
        self.url = url
        self.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accepts': 'text/html,application/xhtml+xml,application/xml',
        }

        self.col_name = col_name

    def get_proxy_list(self):
        proxies_req =  urllib.request.Request('https://www.sslproxies.org/')
        proxies_req.add_header('User-Agent', self.ua.random)
        proxies_doc =  urllib.request.urlopen(proxies_req).read().decode('utf8')
        soup = BeautifulSoup(proxies_doc, 'html.parser')
        proxies_table = soup.find(id='proxylisttable')

        for row in proxies_table.tbody.find_all('tr'):
            self.proxies.append({
            'ip':   row.find_all('td')[0].string,
            'port': row.find_all('td')[1].string
            })

    def random_proxy(self, proxies):
        return random.randint(0, len(proxies) - 1)

    def get_url_list(self):
        pass

    def get_initials(self, url):
        pass

    def fetch_url(self, url, num_retries=10):
        try:	
            initials = self.get_initials(url)
            logging.info(initials)

            # Wait a random time to avoid be blocked by the server
            time.sleep(random.random()*self.sleep_time)		

            # Choose a random proxy to avoid be blocked by the server
            proxy_index = self.random_proxy(self.proxies)
            proxy = self.proxies[proxy_index]

            req = urllib.request.Request(url)
            req.set_proxy(proxy['ip'] + ':' + proxy['port'], 'http')
            handler  = urllib.request.urlopen(url)
            response = handler.read().decode('utf8')
            soup = BeautifulSoup(response, 'html.parser')

            # Retry in case of broken page
            if (soup.find('span').text == 'Sistema indisponivel.') and (num_retries > 0):
                logging.debug(initials + ', retrying...' + str(num_retries))
                return self.fetch_url(url, num_retries-1)

            # Save HTML page
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

            with open(self.output_dir+initials+'.html', 'w', encoding='utf-8') as fo:
                fo.write(str(soup))

        except Exception as e:		
            logging.error(initials + ': ' + str(e))
            if num_retries > 0:
                logging.debug(initials + ', retrying...' + str(num_retries))
                return self.fetch_url(url, num_retries-1)

    def start_crawling(self):
        threads = [threading.Thread(target=self.fetch_url, args=(url,)) for url in self.urls]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.convert_df_to_excel()

    def convert_html_to_df(self, html):
        pass

    def convert_df_to_excel(self):
        df = pd.DataFrame(columns=self.col_name)
        pages = glob.glob(self.output_dir+'*.html')

        for page in pages:
            with open(page, 'r', encoding='utf-8') as fo:		
                df = df.append(self.convert_html_to_df(fo.read()), ignore_index=True, sort=False)
                
        df.to_excel(self.output_file+".xlsx")

class FII(B3):
    def get_url_list(self):
        #self.urls = [line.rstrip('\n') for line in open(self.output_file+'.txt')]
        req = urllib.request.Request(url, headers=self.headers)
        handler = urllib.request.urlopen(req)
        response = handler.read().decode('utf8')
        soup = BeautifulSoup(response, 'html.parser')
        self.urls = [base_url+a['href'] for a in soup.find_all('a', id=re.compile('.RazaoSocial'), href=True) if a.text]

    def get_initials(self, url):
        return url[79:83]

    def convert_html_to_df(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        df = pd.read_html(str(table))[0]
        df = df.append({0 : 'Nome do Fundo', 1 : soup.find_all('h2')[0].get_text()}, ignore_index=True)
        df = df.T
        df.columns = df.iloc[0] 
        df = df[1:]
        return df

class Stock(B3):
    def get_url_list(self):
        # manually collected because there is a button action in the page
        self.urls = [line.rstrip('\n') for line in open(self.output_file+'.txt')]

    def get_initials(self, url):
        return re.findall(r'CodCVM=(\d+)', url)[0]

    def convert_html_to_df(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        df = pd.read_html(str(table))[0]
        df = df.T
        df.columns = df.iloc[0] 
        df = df[1:]
        df['Códigos de Negociação:'] = df['Códigos de Negociação:'].apply(lambda text : ','.join(set(re.findall(r' [A-Z]{4}[0-9]{1,2}', text)))[1:])
        return df

if __name__ == '__main__':
     # Get all FIIs info available in B3 website
    output_file = 'fiis'
    base_url = 'http://bvmf.bmfbovespa.com.br/Fundos-Listados/'
    url = 'http://bvmf.bmfbovespa.com.br/Fundos-Listados/FundosListados.aspx?tipoFundo=imobiliario&Idioma=pt-br'
    col_name = ['Nome do Fundo', 'Nome de Pregão', 'Códigos de Negociação', 'CNPJ', 'Classificação Setorial', 'Site']
    fii = FII(output_file, base_url, url, col_name)
    fii.get_proxy_list()
    fii.get_url_list()
    fii.start_crawling()

    # Get all stocks info available in B3 website
    output_file = 'stocks'
    base_url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM='
    url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/BuscaEmpresaListada.aspx?idioma=pt-br'
    col_name = ['Nome de Pregão:', 'Códigos de Negociação:', 'CNPJ:', 'Atividade Principal:', 'Classificação Setorial:', 'Site:']
    stock = Stock(output_file, base_url, url, col_name)
    stock.get_proxy_list()
    stock.get_url_list()
    stock.start_crawling()
