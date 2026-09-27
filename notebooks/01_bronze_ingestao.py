# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Camada Bronze — Ingestão do dado bruto
# MAGIC
# MAGIC **Objetivo:** levar os 9 arquivos CSV do Volume `mvp.landing.olist_raw` para tabelas **Delta** no schema `mvp.bronze`,
# MAGIC preservando o conteúdo exatamente como veio da fonte (o "cofre de evidências" da arquitetura medalhão).
# MAGIC
# MAGIC **Decisões técnicas**
# MAGIC
# MAGIC | Decisão | Motivo |
# MAGIC |---|---|
# MAGIC | Todas as colunas lidas como `STRING` (`inferSchema = false`) | Não perder nenhum valor por erro de conversão. A tipagem é responsabilidade da Silver. |
# MAGIC | `multiLine = true` e `escape = '"'` | O arquivo de avaliações tem comentários de texto livre com quebras de linha e aspas. |
# MAGIC | Colunas de controle `_arquivo_origem` e `_data_ingestao` | Rastreabilidade (linhagem): de qual arquivo e quando cada linha foi carregada. |
# MAGIC | Única alteração: limpeza do **nome** das colunas (remoção de BOM/espaços) | Um CSV traz o caractere invisível BOM (`﻿`) no cabeçalho, o que quebraria as consultas. O **conteúdo** não é alterado. |
# MAGIC | Modo `overwrite` | Carga completa (*full load*): a fonte é um arquivo estático, reprocessar é idempotente. |

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "mvp"
VOLUME_PATH = f"/Volumes/{CATALOG}/landing/olist_raw"

# arquivo de origem -> (tabela bronze, descrição da tabela para o catálogo)
FONTES = {
    "olist_customers_dataset.csv": ("clientes",
        "Bronze | Clientes da Olist (1 linha por customer_id, que é gerado a cada pedido). Fonte: olist_customers_dataset.csv (Kaggle)."),
    "olist_geolocation_dataset.csv": ("geolocalizacao",
        "Bronze | Coordenadas (lat/lng) por prefixo de CEP. Fonte: olist_geolocation_dataset.csv (Kaggle). Não utilizada nas camadas seguintes (fora do escopo das perguntas)."),
    "olist_order_items_dataset.csv": ("itens_pedido",
        "Bronze | Itens de cada pedido (produto, vendedor, preço e frete). Fonte: olist_order_items_dataset.csv (Kaggle)."),
    "olist_order_payments_dataset.csv": ("pagamentos",
        "Bronze | Pagamentos dos pedidos (tipo, parcelas e valor). Fonte: olist_order_payments_dataset.csv (Kaggle)."),
    "olist_order_reviews_dataset.csv": ("avaliacoes",
        "Bronze | Avaliações dos clientes após a entrega (nota 1-5 e comentários). Fonte: olist_order_reviews_dataset.csv (Kaggle)."),
    "olist_orders_dataset.csv": ("pedidos",
        "Bronze | Pedidos com status e datas do ciclo de vida (compra, aprovação, envio, entrega, estimativa). Fonte: olist_orders_dataset.csv (Kaggle)."),
    "olist_products_dataset.csv": ("produtos",
        "Bronze | Catálogo de produtos (categoria e dimensões). Fonte: olist_products_dataset.csv (Kaggle)."),
    "olist_sellers_dataset.csv": ("vendedores",
        "Bronze | Vendedores parceiros da Olist e sua localização. Fonte: olist_sellers_dataset.csv (Kaggle)."),
    "product_category_name_translation.csv": ("traducao_categorias",
        "Bronze | Tradução do nome das categorias de produto (português -> inglês). Fonte: product_category_name_translation.csv (Kaggle)."),
}

# COMMAND ----------

def limpar_nome_coluna(nome: str) -> str:
    """Remove BOM e espaços do NOME da coluna (o conteúdo não é alterado)."""
    return nome.replace("﻿", "").strip().lower()


resumo = []
for arquivo, (tabela, descricao) in FONTES.items():
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "false")   # tudo como STRING na Bronze
          .option("multiLine", "true")      # comentários de avaliação com quebra de linha
          .option("quote", '"')
          .option("escape", '"')
          .option("encoding", "UTF-8")
          .csv(f"{VOLUME_PATH}/{arquivo}"))

    df = df.toDF(*[limpar_nome_coluna(c) for c in df.columns])

    df = (df
          .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
          .withColumn("_data_ingestao", F.current_timestamp()))

    nome_completo = f"{CATALOG}.bronze.{tabela}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(nome_completo))

    spark.sql(f"COMMENT ON TABLE {nome_completo} IS '{descricao}'")
    spark.sql(f"ALTER TABLE {nome_completo} ALTER COLUMN _arquivo_origem COMMENT 'Metadado de linhagem: caminho do arquivo CSV de origem no Volume.'")
    spark.sql(f"ALTER TABLE {nome_completo} ALTER COLUMN _data_ingestao COMMENT 'Metadado de controle: data/hora em que a linha foi carregada na Bronze.'")

    qtd = spark.table(nome_completo).count()
    resumo.append((arquivo, nome_completo, len(df.columns) - 2, qtd))
    print(f"{nome_completo:35s} <- {arquivo:40s} {qtd:>10,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência da carga
# MAGIC Tabela-resumo das tabelas Bronze persistidas (volume de linhas e colunas de negócio de cada fonte).

# COMMAND ----------

display(spark.createDataFrame(resumo, "arquivo_origem string, tabela_bronze string, qtd_colunas_negocio int, qtd_linhas long"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN mvp.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Amostra do dado bruto: repare que tudo é STRING e que as datas ainda são texto
# MAGIC SELECT * FROM mvp.bronze.pedidos LIMIT 10
