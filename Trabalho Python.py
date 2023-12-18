import subprocess
import sys
import os
import json
import zipfile
import pandas as pd 
import sqlite3


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install('kaggle')


# Caminho onde o kaggle.json será armazenado
kaggle_config_path = '/root/.kaggle/kaggle.json' 

# Verificar se o diretório pai existe, se não, criar
os.makedirs(os.path.dirname(kaggle_config_path), exist_ok=True)

# Escrever o conteúdo no arquivo kaggle.json
with open(kaggle_config_path, 'w') as kaggle_config_file:
    kaggle_config_file.write(kaggle_config_path)

# Tornar o arquivo kaggle.json seguro
os.chmod(kaggle_config_path, 600)

# Validando se a biblioteca kaggle realmente esta instalada
os.makedirs(os.path.join(os.path.expanduser('~'), '.kaggle'), exist_ok=True)

# Escrevendo .json file do kaggle
with open(os.path.join(os.path.expanduser('~'), '.kaggle/kaggle.json'), 'w') as file:
    json.dump(kaggle_config_path, file)

# Setando permissões
os.chmod(os.path.join(os.path.expanduser('~'), '.kaggle/kaggle.json'), 0o600)

def download_kaggle_dataset(dataset):
    subprocess.run(['kaggle', 'datasets', 'download', '-d', dataset])

# Faz o dowload dos dados
download_kaggle_dataset('olistbr/brazilian-ecommerce')

# Descompacta o arquivo zip carregado pela api
with zipfile.ZipFile('brazilian-ecommerce.zip', 'r') as zip_ref:
    zip_ref.extractall('brazilian_ecommerce')


# Lê as informações do arquivo .zip gerado acima e e carrega em um DataFrame 
df_tb_olist_orders_dataset = pd.read_csv('brazilian_ecommerce/olist_orders_dataset.csv')
print(df_tb_olist_orders_dataset.head())

df_olist_order_items_dataset = pd.read_csv('brazilian_ecommerce/olist_order_items_dataset.csv')
print(df_olist_order_items_dataset.head())

df_olist_order_payments_dataset = pd.read_csv('brazilian_ecommerce/olist_order_payments_dataset.csv')
print(df_olist_order_payments_dataset.head())

df_olist_order_reviews_dataset = pd.read_csv('brazilian_ecommerce/olist_order_reviews_dataset.csv')
print(df_olist_order_reviews_dataset.head())

df_olist_customers_dataset = pd.read_csv('brazilian_ecommerce/olist_customers_dataset.csv')
print(df_olist_customers_dataset.head())

df_olist_products_dataset = pd.read_csv('brazilian_ecommerce/olist_products_dataset.csv')
print(df_olist_products_dataset.head())

df_olist_sellers_dataset = pd.read_csv('brazilian_ecommerce/olist_sellers_dataset.csv')
print(df_olist_sellers_dataset.head())

df_product_category_name_translation = pd.read_csv('brazilian_ecommerce/product_category_name_translation.csv')
print(df_product_category_name_translation.head())

df_olist_orders_dataset = pd.read_csv('brazilian_ecommerce/olist_orders_dataset.csv')
print(df_olist_orders_dataset.head())


# Conectar ao banco de dados (o banco será criado se não existir)
# Cria o SQLAlchemy da engine
engine = sqlite3.connect('brazilian_ecommerce.db')

# Salva o dataset no banco de dadfos
df_olist_orders_dataset.to_sql('olist_orders_dataset', con=engine, index=False, if_exists='replace')
df_olist_order_items_dataset.to_sql('olist_order_items_dataset', con=engine, index=False, if_exists='replace')
df_olist_order_payments_dataset.to_sql('olist_order_payments_dataset', con=engine, index=False, if_exists='replace')
df_olist_order_reviews_dataset.to_sql('olist_order_reviews_dataset', con=engine, index=False, if_exists='replace')
df_olist_customers_dataset.to_sql('olist_customers_dataset', con=engine, index=False, if_exists='replace')
df_olist_products_dataset.to_sql('olist_products_dataset', con=engine, index=False, if_exists='replace')
df_olist_sellers_dataset.to_sql('olist_sellers_dataset', con=engine, index=False, if_exists='replace')
df_product_category_name_translation.to_sql('product_category_name_translation', con=engine, index=False, if_exists='replace')
df_olist_orders_dataset.to_sql('olist_orders_dataset', con=engine, index=False, if_exists='replace')


#Exemplo de consulta do banco de dados
cursor = engine.cursor()

cursor.execute('SELECT * FROM olist_orders_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)


cursor.execute('SELECT * FROM olist_order_items_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM olist_order_payments_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM olist_order_reviews_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM olist_customers_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM olist_products_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM olist_sellers_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM product_category_name_translation LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

cursor.execute('SELECT * FROM olist_orders_dataset LIMIT 1')
dados = cursor.fetchall()

for linha in dados:
    print(linha)

# Fechar a conexão
engine.close()