# importando as bibliotecas

import sqlite3
from google.cloud import storage 
import numpy as np
import mysql.connector
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from datetime import date
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# define a data atual para inserção no dataframe

data_atual = datetime.now()
data_atual_formatada = data_atual.strftime('%d/%m/%Y %H:%M:%S')

# definir os produtos para consulta e a variavel de append

produtos_consulta = ['iphone 12', 'BRE57AK', 'BRM54HK', 'logitech g29', 'WD11M4453JX', 'AOC 27 Hero 144HZ', 'CD060AE', 'RYZEN 5 5600x' , 'asus prime a520m']
#produtos_consulta = ['AOC 27 Hero 144HZ']
lista_dados_produto = []
lista_aval = []
produto_consultado = []

## Instanciando o navegador e pegando os dados via xPath
options = webdriver.ChromeOptions()
options.add_argument('headless')

for p in produtos_consulta:
    navegador = webdriver.Chrome(executable_path=r"C:\Users\luuca\Downloads\chromedriver.exe", chrome_options=options)
    navegador.get("https://www.google.com/")
    navegador.find_element_by_xpath('/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div[2]/div[2]/input').send_keys(p + Keys.ENTER)
    
    bloco_valores = navegador.find_elements_by_xpath("//*[starts-with(@class, 'mnr-c pla-unit')]")
    bloco_links = navegador.find_elements_by_xpath("//*[starts-with(@class,'plantl tkXAec')]")
    bloco_aval = navegador.find_elements_by_xpath("//*[starts-with(@class,'pla-extensions-container')]")
    
    # preparando a lista com os dados que vamos utilizar
    i = 0
    while i < len(bloco_valores):
        lista_dados_produto.append(bloco_valores[i].text + '\n' + bloco_links[i].get_attribute("href") + '\n' +  p)
        lista_aval.append(bloco_aval[i].text)
        #print(bloco_valores[i].text + '\n' + links[i])
        i += 1
        
    cont= 0
    lista_corrigida = []
    while cont < len(lista_dados_produto):
        lista_corrigida.append(lista_dados_produto[cont].replace(lista_aval[cont],'').replace('\n\n','\n'))
        #print(lista_dados_produto[cont])
        #print(lista_aval[cont])
        cont = cont + 1
            
# Criando a series para inserir no Data Frame

series_dados_produto = pd.Series(data = lista_corrigida).str.replace('PROMOÇÃO\n','', regex = True).str.replace('Retirar amanhã\n','',regex = True).str.replace('Retirar hoje\n','',regex = True).str.replace('Na loja\n','', regex = True).str.replace('REDUÇÃO NO PREÇO\n', '', regex = True).str.replace('(','', regex = True).str.replace(')','', regex = True).str.replace('+','', regex = True).str.replace('4k\n','', regex = True).str.replace('3k\n','', regex = True).str.replace('2k\n','', regex = True).str.replace('1k\n','', regex = True).str.replace('5k\n','', regex = True).str.replace('6k\n','', regex = True).str.replace("'", '', regex = True).str.split('\n', expand=True)

#criação e tratamento do DF

dados_produto_df = pd.DataFrame(series_dados_produto).rename(columns = {0:'nome_produto', 1:'valor_produto', 2:'loja', 3:'link_compra', 4:'produto'}, inplace = False)
dados_produto_df = dados_produto_df[['loja', 'nome_produto', 'valor_produto', 'link_compra', 'produto']]
#dados_produto_df['valor_produto'] = pd.to_numeric(dados_produto_df['valor_produto'].str[3:11].str.replace('.','', regex = True).str.replace(',' , '.' ,regex = True))
dados_produto_df['valor_produto'] = pd.to_numeric(dados_produto_df['valor_produto'].str[3:].str.split(',').str[0].str.replace('.','',regex = True) + '.' + dados_produto_df['valor_produto'].str.split(',').str[1].str[:2])
dados_produto_df['data_consulta'] = data_atual_formatada
dados_produto_df

# timestamp do aqruivo

data_arquivo = data_atual.strftime('%d_%m_%Y_%H_%M_%S')
caminho = 'dados_'+data_arquivo+'.csv'

# criação do arquivo local para backup

dados_produto_df.to_csv(caminho,index=False)

#### instanciando conexão com o banco ####

cnx = mysql.connector.connect(user='root',password='senha', host='ip', database='banco')
#cnx = mysql.connector.connect(user='seu usuario',password='sua senha', host='seu host', database='seu banco')

cursor = cnx.cursor()

# loop de inserção no banco

temp = 0
for i in range(len(dados_produto_df)):
    loja = str(dados_produto_df['loja'].iloc[i])
    nome_produto = str(dados_produto_df['nome_produto'].iloc[i])
    valor_produto = dados_produto_df['valor_produto'].iloc[i]
    link_compra = str(dados_produto_df['link_compra'].iloc[i])
    data_consulta = str(dados_produto_df['data_consulta'].iloc[i])
    produto = str(dados_produto_df['produto'].iloc[i])
    #print(loja)
    #print(nome_produto)
    #print(valor_produto)
    #print(link_compra)
    
    inserir_dados = ("INSERT INTO TB_PRODUTOS(LOJA, NOME_PRODUTO, VALOR_PRODUTO, LINK_COMPRA, DATA_CONSULTA, PRODUTO)" 
              "values (%s, %s, %s, %s, %s, %s)")
    valores = (loja, nome_produto, valor_produto, link_compra, data_consulta, produto)
    cursor.execute(inserir_dados, valores)
    cnx.commit()
    print(temp, "Record Inserted for ", loja)
    temp = temp + 1

#fechar a conexão

cnx.close()
