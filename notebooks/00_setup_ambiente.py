# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente (Unity Catalog)
# MAGIC
# MAGIC **MVP – Engenharia de Dados (PUC-Rio) · Pipeline de dados do e-commerce Olist**
# MAGIC
# MAGIC Este notebook prepara a estrutura lógica do Lakehouse no **Unity Catalog** do Databricks Free Edition:
# MAGIC
# MAGIC | Objeto | Nome | Papel na arquitetura medalhão |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `mvp` | Contêiner de todo o projeto |
# MAGIC | Schema | `mvp.landing` | Área de pouso: guarda os **arquivos CSV originais** em um *Volume* |
# MAGIC | Schema | `mvp.bronze` | Dado bruto em tabela Delta, **sem alteração de conteúdo** (+ metadados de ingestão) |
# MAGIC | Schema | `mvp.silver` | Dado limpo, tipado, deduplicado e padronizado |
# MAGIC | Schema | `mvp.gold` | Modelo dimensional (esquema estrela / constelação) pronto para análise |
# MAGIC | Volume | `mvp.landing.olist_raw` | Pasta onde os 9 CSVs do Kaggle são enviados (upload manual) |
# MAGIC
# MAGIC > **Ordem de execução do pipeline:** `00_setup_ambiente` → `01_bronze_ingestao` → `02_silver_transformacao` → `03_gold_modelagem` → `04_qualidade_dados` → `05_analise_negocio`

# COMMAND ----------

CATALOG = "mvp"

# Na Free Edition é possível criar catálogos. Caso a criação falhe por permissão,
# o pipeline usa o catálogo padrão "workspace" (basta trocar a variável CATALOG nos notebooks).
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'MVP PUC-Rio - Pipeline de dados do e-commerce brasileiro Olist (arquitetura medalhão)'")
except Exception as e:
    print(f"Não foi possível criar o catálogo '{CATALOG}': {e}")
    CATALOG = "workspace"

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Catálogo em uso: {CATALOG}")

# COMMAND ----------

schemas = {
    "landing": "Área de pouso: arquivos originais (CSV) exatamente como baixados do Kaggle, armazenados em Volume.",
    "bronze":  "Camada Bronze: tabelas Delta com o dado bruto dos CSVs (todas as colunas como STRING) + metadados de ingestão.",
    "silver":  "Camada Silver: dados limpos, tipados, deduplicados e padronizados, uma tabela por entidade de negócio.",
    "gold":    "Camada Gold: modelo dimensional (fatos e dimensões) pronto para responder às perguntas de negócio.",
}
for nome, comentario in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{nome} COMMENT '{comentario}'")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.olist_raw
    COMMENT 'Arquivos CSV originais do dataset Brazilian E-Commerce Public Dataset by Olist (Kaggle, CC BY-NC-SA 4.0).'
""")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload dos arquivos (passo manual)
# MAGIC
# MAGIC 1. Baixar o zip em <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce> e extrair os 9 CSVs.
# MAGIC 2. No Databricks: **Catalog → mvp → landing → Volumes → olist_raw → Upload to this volume** e enviar os 9 arquivos.
# MAGIC 3. Rodar a célula abaixo para conferir se todos chegaram.

# COMMAND ----------

ARQUIVOS_ESPERADOS = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]
VOLUME_PATH = f"/Volumes/{CATALOG}/landing/olist_raw"

presentes = {f.name: f.size for f in dbutils.fs.ls(VOLUME_PATH)}
for arq in ARQUIVOS_ESPERADOS:
    status = f"OK  ({presentes[arq] / 1024 / 1024:.1f} MB)" if arq in presentes else "FALTANDO"
    print(f"{arq:45s} {status}")
