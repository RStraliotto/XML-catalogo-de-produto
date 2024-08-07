# Projeto de Ingestão e Transformação de Dados com Delta Lake

## Descrição

Este projeto tem como objetivo a ingestão de dados de um arquivo XML, a transformação e o armazenamento desses dados em uma tabela Delta Lake no Databricks. O processo inclui a criação de uma tabela bronze para armazenar os dados brutos, a transformação dos dados para uma camada prata e a agregação dos dados para uma camada ouro.

## Estrutura do Projeto

- **Ingestão de Dados**: O script faz a requisição de um arquivo XML, analisa o conteúdo e cria um DataFrame com os dados extraídos.
- **Armazenamento em Tabela Bronze**: O DataFrame é salvo em uma tabela Delta Lake na camada bronze.
- **Transformação e Armazenamento em Tabela Prata**: Os dados são transformados (tipos de dados ajustados) e salvos em uma tabela Delta Lake na camada prata.
- **Agregação e Armazenamento em Tabela Ouro**: Dados são agregados e salvos em uma tabela Delta Lake na camada ouro para análise e visualização.

  ![dashboard_xml_Double_Flip_skate_shop](https://github.com/user-attachments/assets/2412fa27-e06d-4d01-976a-bd3bcfc74d80)
  ![table_xml_Double_Flip_skate_shop](https://github.com/user-attachments/assets/f504998a-6a36-44d7-ad85-5aa60dd2b187)



## Requisitos

- **Apache Spark**: A versão 3.0 ou superior é recomendada.
- **Delta Lake**: Certifique-se de que o Delta Lake está configurado no seu ambiente Databricks.
- **Bibliotecas**: `requests`, `pyspark`, `xml.etree.ElementTree`.

## Instalação

1. **Instale as bibliotecas necessárias**:
    ```bash
    pip install requests pyspark
    ```

2. **Configure o Delta Lake no Databricks**:
   - Certifique-se de que o Delta Lake está habilitado e configurado em seu cluster Databricks.

## Uso

1. **Criação e Atualização de Tabela Delta**:
    - Execute o seguinte script para criar ou atualizar a tabela Delta bronze.meli:

    ```python
    import xml.etree.ElementTree as ET
    import requests
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from delta.tables import DeltaTable

    # Criar sessão Spark
    spark = SparkSession.builder \
        .appName("XML Data Ingestion and Incremental Load") \
        .getOrCreate()

    # Definir URL do XML
    xml_url = "url da sua loja nuvemshop"

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
    ```

## Consultas

Para consultar os dados em diferentes camadas, você pode usar as seguintes consultas SQL:

1. **Consulta na Tabela Bronze**:
    ```sql
    SELECT * FROM bronze.meli
    ```

2. **Consulta na Tabela Prata**:
    ```sql
    SELECT * FROM silver.meli
    ```

3. **Consulta na Tabela Ouro**:
    ```sql
    SELECT * FROM gold.meli
    ```

## Contribuições

Sinta-se à vontade para contribuir com melhorias ou correções. Por favor, faça um fork do repositório e envie um pull request com suas alterações.

## Licença

Este projeto está licenciado sob a Licença MIT - consulte o arquivo [LICENSE](LICENSE) para mais detalhes.
