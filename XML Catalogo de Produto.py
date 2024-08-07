# Databricks notebook source
# MAGIC %md
# MAGIC Extração e Carregamento Incremental Inicial (Tabela bronze.meli)

# COMMAND ----------

import xml.etree.ElementTree as ET
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("XML Data Ingestion and Incremental Load") \
    .getOrCreate()

# Definir URL do XML
xml_url = "url do xml catalogo de produto sua loja numveshop"

# Fazer a requisição HTTP e obter o conteúdo XML
response = requests.get(xml_url)
xml_content = response.content

# Analisar o XML
root = ET.fromstring(xml_content)

# Limitar o número de itens processados
max_items = 50  # limite de itens para teste
data = []

# Extrair itens do XML com limite
for item in root.findall('channel/item')[:max_items]:
    product_data = {
        'product_id': item.find('id').text,
        'title': item.find('title').text,
        'price': item.find('price').text,
        'sale_price': item.find('sale_price').text,
        'brand': item.find('brand').text,
    }
    data.append(product_data)

# Criar DataFrame com os dados extraídos
df_new = spark.createDataFrame(data)

# Criar banco de dados "bronze" se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Nome da tabela
table_name = "bronze.meli"

# Verificar se a tabela Delta existe
if not spark.catalog.tableExists(table_name):
    # Se a tabela não existir, criar a tabela Delta com os dados
    df_new.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print("Tabela bronze.meli criada e dados iniciais inseridos.")
else:
    # Se a tabela existir, fazer o merge dos dados
    from delta.tables import DeltaTable
    
    # Converter o DataFrame existente em DeltaTable
    delta_table = DeltaTable.forName(spark, table_name)

    # Executar o merge
    delta_table.alias("existing").merge(
        df_new.alias("new"),
        "existing.product_id = new.product_id"
    ).whenMatchedUpdate(
        condition="existing.title != new.title OR existing.price != new.price OR existing.sale_price != new.sale_price OR existing.brand != new.brand",
        set={
            "title": col("new.title"),
            "price": col("new.price"),
            "sale_price": col("new.sale_price"),
            "brand": col("new.brand")
        }
    ).whenNotMatchedInsert(
        values={
            "product_id": col("new.product_id"),
            "title": col("new.title"),
            "price": col("new.price"),
            "sale_price": col("new.sale_price"),
            "brand": col("new.brand")
        }
    ).execute()

    print("Dados atualizados na tabela bronze.meli.")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.meli

# COMMAND ----------

# Criar consulta SQL para selecionar todos os dados da tabela "meli" no banco de dados "bronze"
#query = "SELECT * FROM bronze.meli"

# Executar a consulta e obter o resultado como DataFrame
#df_result = spark.sql(query)

# Exibir o esquema do DataFrame resultante
#df_result.printSchema()

# Mostrar os dados (exibindo as primeiras 20 linhas por padrão)
#df_result.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Criar sessão Spark (não necessário no Databricks, mas incluído para completude)
spark = SparkSession.builder \
    .appName("Data Transformation and Loading") \
    .getOrCreate()

# Ler os dados da tabela bronze
df_bronze = spark.sql("SELECT * FROM bronze.meli")

# Transformar os dados
data_transformed = df_bronze.rdd.map(lambda row: {
    'product_id': int(row['product_id']),
    'title': row['title'],
    'price': float(row['price'].replace(' BRL', '').replace(',', '.')),
    'sale_price': float(row['sale_price'].replace(' BRL', '').replace(',', '.')),
    'brand': row['brand'],
}).collect()

# Definir o esquema para o DataFrame transformado
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("sale_price", FloatType(), True),
    StructField("brand", StringType(), True)
])

# Criar DataFrame com o esquema definido
df_transformed = spark.createDataFrame(data_transformed, schema)

# Criar banco de dados "prata" se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS prata")

# Salvar os dados na tabela Delta Lake "meli" no banco de dados "prata"
df_transformed.write.format("delta").mode("overwrite").saveAsTable("prata.meli")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prata.meli

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

# Criar sessão Spark (não necessário no Databricks, mas incluído para completude)
spark = SparkSession.builder \
    .appName("Carrega resultado") \
    .getOrCreate()

# Ler os dados da tabela prata
df_prata = spark.sql("SELECT * FROM prata.meli")

# Agregar os dados: calcular a média dos preços e preços de venda por marca
df_summary = df_prata.groupBy("brand").agg(
    round(avg("price"), 2).alias("average_price"),
    round(avg("sale_price"), 2).alias("average_sale_price")
)

# Criar banco de dados "ouro" se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS ouro")

# Salvar os dados agregados na tabela Delta Lake "meli_aggregated" no banco de dados "ouro"
df_summary.write.format("delta").mode("overwrite").saveAsTable("ouro.meli_summary")


# COMMAND ----------

# Criar consulta SQL para selecionar todos os dados da tabela "meli_aggregated" no banco de dados "ouro"
query = "SELECT * FROM ouro.meli_summary"

# Executar a consulta e obter o resultado como DataFrame
df_ouro = spark.sql(query)

# Exibir o esquema do DataFrame resultante
df_ouro.printSchema()

# Mostrar os dados (exibindo as primeiras 20 linhas por padrão)
df_ouro.show()

