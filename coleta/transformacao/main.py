#imports

import pandas as pd
import sqlite3
from datetime import datetime

# Caminho do arquivo json

df = pd.read_json('/home/mor/Desktop/Cursos/WebScraping_Scrapy/data/data.jsonl', lines=True)

# Fazer o pandas mostras todas as colunas
pd.options.display.max_columns = None
# Adcionar colunas 

df['_source'] = 'https://lista.mercadolivre.com.br/tenis-corrida-masculino'
df['_data_coleta'] = datetime.now()

# Tratar Na para colunas numéricas e strings:
df['old_price_reais'] = df['old_price_reais'].fillna(0).astype(float)
df['old_price_centavos'] = df['old_price_centavos'].fillna(0).astype(float)
df['new_price_reais'] = df['new_price_reais'].fillna(0).astype(float)
df['new_price_centavos'] = df['new_price_centavos'].fillna(0).astype(float)
df['reviews_rating_number'] = df['reviews_rating_number'].fillna(0).astype(float)

# remover os parênteses das colunas reviews_amount

df['reviews_amount'] = df['reviews_amount'].str.replace('[\(\)]', '', regex=True)
df['reviews_amount'] = df['reviews_amount'].fillna(0).astype(int)

# Tratar os preços como floats e calcular os valores totais

df['old_price'] = df['old_price_reais'] + df['old_price_centavos'] / 100
df['new_price'] = df['new_price_reais'] + df['new_price_centavos'] / 100

# remover as colunas antigas de preços

df.drop(columns=['old_price_reais', 'old_price_centavos', 'new_price_reais', 'new_price_centavos'], inplace=True)

# conectar ao banco de dados sqlite

conn = sqlite3.connect('/home/mor/Desktop/Cursos/WebScraping_Scrapy/data/quotes.db')# cria um db na pasta data

# Salva o dataframe no banco sqlite criado acima

df.to_sql('mercadolivre_items', conn, if_exists='replace', index=False) #mercadolivre_items é o nome da tabela criada

# fecha a conexão com o banco de dados

conn.close()

print(df.head())


