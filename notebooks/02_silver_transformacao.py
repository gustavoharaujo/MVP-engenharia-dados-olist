# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Camada Silver — Limpeza, tipagem e padronização
# MAGIC
# MAGIC **Objetivo:** transformar cada tabela Bronze (tudo `STRING`, dado cru) em uma tabela Silver confiável, uma por entidade de negócio.
# MAGIC
# MAGIC Transformações aplicadas (o detalhe de cada uma está nas células abaixo):
# MAGIC
# MAGIC 1. **Renomeação** das colunas para português em `snake_case` (ex.: `order_purchase_timestamp` → `data_compra`), corrigindo inclusive o erro de digitação da fonte (`product_name_lenght`).
# MAGIC 2. **Tipagem**: textos de data → `TIMESTAMP`; valores monetários → `DECIMAL(12,2)`; contagens e notas → `INT`. Uso de `try_cast` para que um valor mal formatado vire nulo em vez de derrubar o pipeline.
# MAGIC 3. **Padronização**: `trim`, UF em maiúsculas, cidade em minúsculas sem acento, CEP com 5 dígitos, status e tipos de pagamento em minúsculas.
# MAGIC 4. **Deduplicação** pela chave natural de cada entidade.
# MAGIC 5. **Tratamento de nulos e valores inválidos** (categorias sem nome, parcelas = 0, notas fora de 1–5, preços ≤ 0).
# MAGIC 6. **Enriquecimento simples**: join de produtos com a tabela de tradução de categorias.
# MAGIC
# MAGIC Ao final, cada transformação tem seu **impacto registrado** na tabela `mvp.silver.controle_carga` (linhas lidas × gravadas × descartadas).
# MAGIC
# MAGIC > A tabela `geolocalizacao` **não** é levada para a Silver: nenhuma pergunta de negócio depende de coordenadas geográficas (a análise regional usa a UF). Ela permanece na Bronze para usos futuros.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "mvp"
B = f"{CATALOG}.bronze"
S = f"{CATALOG}.silver"

UFS_VALIDAS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE",
               "PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

controle = []  # (tabela, linhas_bronze, linhas_silver, descartadas, observacao)


def ts(col):
    """Texto 'yyyy-MM-dd HH:mm:ss' -> TIMESTAMP (valor inválido vira NULL)."""
    return F.expr(f"try_to_timestamp(trim({col}), 'yyyy-MM-dd HH:mm:ss')")


def num(col, tipo):
    """Conversão numérica segura: valor inválido vira NULL em vez de gerar erro."""
    return F.expr(f"try_cast(trim({col}) AS {tipo})")


def cidade(col):
    """Minúsculas, sem acentos e sem espaços duplicados."""
    return F.regexp_replace(
        F.translate(F.lower(F.trim(F.col(col))),
                    "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"),
        r"\s+", " ")


def uf(col):
    """UF em maiúsculas; se não for uma das 27 UFs válidas, vira NULL."""
    u = F.upper(F.trim(F.col(col)))
    return F.when(u.isin(UFS_VALIDAS), u)


def gravar(df, tabela, descricao, comentarios_colunas, linhas_bronze, observacao):
    nome = f"{S}.{tabela}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome)
    spark.sql(f"COMMENT ON TABLE {nome} IS '{descricao}'")
    for coluna, texto in comentarios_colunas.items():
        spark.sql(f"ALTER TABLE {nome} ALTER COLUMN {coluna} COMMENT '{texto}'")
    linhas_silver = spark.table(nome).count()
    controle.append((nome, linhas_bronze, linhas_silver, linhas_bronze - linhas_silver, observacao))
    print(f"{nome:32s} bronze={linhas_bronze:>9,}  silver={linhas_silver:>9,}  descartadas={linhas_bronze - linhas_silver:>7,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Clientes
# MAGIC - `customer_id` é gerado **a cada pedido**; a pessoa real é identificada por `customer_unique_id` (mantemos os dois).
# MAGIC - CEP prefixo garantido com 5 dígitos via `lpad` (ex.: `1151` → `01151`). No CSV original os CEPs já vêm com zero à esquerda (0 ocorrências, ver notebook 04),
# MAGIC   mas qualquer leitura com inferência de tipo numérico perderia esse zero: a regra é uma **salvaguarda preventiva**.
# MAGIC - Deduplicação por `id_cliente`.

# COMMAND ----------

bronze = spark.table(f"{B}.clientes")
silver = (bronze
    .select(
        F.trim("customer_id").alias("id_cliente"),
        F.trim("customer_unique_id").alias("id_cliente_unico"),
        F.lpad(F.trim("customer_zip_code_prefix"), 5, "0").alias("cep_prefixo"),
        cidade("customer_city").alias("cidade"),
        uf("customer_state").alias("uf"))
    .filter(F.col("id_cliente").isNotNull())
    .dropDuplicates(["id_cliente"]))

gravar(silver, "clientes",
       "Silver | Clientes limpos e padronizados. Linhagem: mvp.bronze.clientes.",
       {"id_cliente": "Chave do cliente no pedido (hash, 32 caracteres). Um novo id é gerado a cada pedido. Origem: customer_id.",
        "id_cliente_unico": "Identificador da pessoa (hash). Permite identificar recompra. Origem: customer_unique_id.",
        "cep_prefixo": "5 primeiros dígitos do CEP (00000-99999), com zero à esquerda garantido. Origem: customer_zip_code_prefix.",
        "cidade": "Cidade em minúsculas e sem acento. Origem: customer_city.",
        "uf": "Sigla da UF (27 valores válidos; inválido vira NULL). Origem: customer_state."},
       bronze.count(), "trim, lpad CEP, padronização cidade/UF, dedup por id_cliente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Pedidos
# MAGIC - Conversão das 5 datas do ciclo de vida de texto para `TIMESTAMP`.
# MAGIC - Datas **nulas não são removidas**: são esperadas em pedidos cancelados ou ainda não entregues (o nulo tem significado de negócio).
# MAGIC - Criação da flag `flag_datas_inconsistentes` para pedidos com cronologia impossível (ex.: entrega antes da compra), que são mantidos mas **marcados** para que a Gold possa excluí-los das métricas de prazo.

# COMMAND ----------

bronze = spark.table(f"{B}.pedidos")
silver = (bronze
    .select(
        F.trim("order_id").alias("id_pedido"),
        F.trim("customer_id").alias("id_cliente"),
        F.lower(F.trim("order_status")).alias("status_pedido"),
        ts("order_purchase_timestamp").alias("data_compra"),
        ts("order_approved_at").alias("data_aprovacao"),
        ts("order_delivered_carrier_date").alias("data_envio_transportadora"),
        ts("order_delivered_customer_date").alias("data_entrega_cliente"),
        ts("order_estimated_delivery_date").alias("data_estimada_entrega"))
    .filter(F.col("id_pedido").isNotNull() & F.col("data_compra").isNotNull())
    .dropDuplicates(["id_pedido"])
    .withColumn("flag_datas_inconsistentes",
        (F.col("data_aprovacao") < F.col("data_compra")) |
        (F.col("data_entrega_cliente") < F.col("data_compra")) |
        ((F.col("status_pedido") == "delivered") & F.col("data_entrega_cliente").isNull()))
    .withColumn("flag_datas_inconsistentes", F.coalesce("flag_datas_inconsistentes", F.lit(False))))

gravar(silver, "pedidos",
       "Silver | Pedidos com datas tipadas e flag de inconsistência cronológica. Linhagem: mvp.bronze.pedidos.",
       {"id_pedido": "Chave do pedido (hash, 32 caracteres). Origem: order_id.",
        "id_cliente": "Cliente do pedido (FK para silver.clientes). Origem: customer_id.",
        "status_pedido": "Status: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved. Origem: order_status.",
        "data_compra": "Data/hora da compra. Período: set/2016 a out/2018. Origem: order_purchase_timestamp.",
        "data_aprovacao": "Data/hora de aprovação do pagamento (pode ser nula). Origem: order_approved_at.",
        "data_envio_transportadora": "Data/hora de postagem na transportadora (pode ser nula). Origem: order_delivered_carrier_date.",
        "data_entrega_cliente": "Data/hora da entrega ao cliente (nula se não entregue). Origem: order_delivered_customer_date.",
        "data_estimada_entrega": "Data prometida ao cliente no momento da compra. Origem: order_estimated_delivery_date.",
        "flag_datas_inconsistentes": "TRUE se a cronologia é impossível (aprovação/entrega antes da compra) ou se status=delivered sem data de entrega. Regra criada na Silver."},
       bronze.count(), "tipagem de 5 datas, dedup por id_pedido, flag de inconsistência cronológica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Itens do pedido
# MAGIC - Preço e frete convertidos para `DECIMAL(12,2)` (tipo adequado para moeda, sem erro de arredondamento de ponto flutuante).
# MAGIC - Descartadas linhas com preço nulo ou ≤ 0 (acurácia: um item vendido não pode ter preço zero).
# MAGIC - Deduplicação pela chave composta (`id_pedido`, `item_seq`).

# COMMAND ----------

bronze = spark.table(f"{B}.itens_pedido")
silver = (bronze
    .select(
        F.trim("order_id").alias("id_pedido"),
        num("order_item_id", "INT").alias("item_seq"),
        F.trim("product_id").alias("id_produto"),
        F.trim("seller_id").alias("id_vendedor"),
        ts("shipping_limit_date").alias("data_limite_envio"),
        num("price", "DECIMAL(12,2)").alias("preco"),
        num("freight_value", "DECIMAL(12,2)").alias("frete"))
    .filter(F.col("preco").isNotNull() & (F.col("preco") > 0) & F.col("frete").isNotNull())
    .dropDuplicates(["id_pedido", "item_seq"]))

gravar(silver, "itens_pedido",
       "Silver | Itens vendidos em cada pedido, com preço e frete tipados. Linhagem: mvp.bronze.itens_pedido.",
       {"id_pedido": "Pedido ao qual o item pertence (FK). Origem: order_id.",
        "item_seq": "Número sequencial do item dentro do pedido (1..21). Origem: order_item_id.",
        "id_produto": "Produto vendido (FK para silver.produtos). Origem: product_id.",
        "id_vendedor": "Vendedor responsável pelo item (FK para silver.vendedores). Origem: seller_id.",
        "data_limite_envio": "Prazo para o vendedor entregar o item à transportadora. Origem: shipping_limit_date.",
        "preco": "Preço do item em R$ (> 0). Origem: price.",
        "frete": "Frete do item em R$ (>= 0). Quando o pedido tem vários itens, o frete é rateado entre eles. Origem: freight_value."},
       bronze.count(), "tipagem DECIMAL, remoção de preço nulo/<=0, dedup por (id_pedido,item_seq)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Pagamentos
# MAGIC - `payment_type = 'not_defined'` padronizado para `nao_definido`.
# MAGIC - **Correção de acurácia:** registros com `payment_installments = 0` passam a ter 1 parcela (todo pagamento tem ao menos uma parcela).
# MAGIC - Deduplicação por (`id_pedido`, `pagamento_seq`). Um pedido pode ter vários pagamentos (ex.: cartão + voucher).

# COMMAND ----------

bronze = spark.table(f"{B}.pagamentos")
silver = (bronze
    .select(
        F.trim("order_id").alias("id_pedido"),
        num("payment_sequential", "INT").alias("pagamento_seq"),
        F.lower(F.trim("payment_type")).alias("tipo_pagamento"),
        num("payment_installments", "INT").alias("parcelas"),
        num("payment_value", "DECIMAL(12,2)").alias("valor_pagamento"))
    .withColumn("tipo_pagamento", F.when(F.col("tipo_pagamento") == "not_defined", "nao_definido").otherwise(F.col("tipo_pagamento")))
    .withColumn("parcelas", F.when(F.col("parcelas") < 1, 1).otherwise(F.col("parcelas")))
    .filter(F.col("id_pedido").isNotNull() & F.col("valor_pagamento").isNotNull())
    .dropDuplicates(["id_pedido", "pagamento_seq"]))

gravar(silver, "pagamentos",
       "Silver | Pagamentos dos pedidos padronizados. Linhagem: mvp.bronze.pagamentos.",
       {"id_pedido": "Pedido pago (FK). Origem: order_id.",
        "pagamento_seq": "Sequência do pagamento dentro do pedido (1..29). Origem: payment_sequential.",
        "tipo_pagamento": "Meio de pagamento: credit_card, boleto, voucher, debit_card, nao_definido. Origem: payment_type.",
        "parcelas": "Número de parcelas (1..24). Valores 0 da fonte corrigidos para 1. Origem: payment_installments.",
        "valor_pagamento": "Valor pago em R$ nesta forma de pagamento. Origem: payment_value."},
       bronze.count(), "padronização tipo_pagamento, parcelas 0->1, dedup por (id_pedido,pagamento_seq)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Avaliações
# MAGIC - Descartadas linhas com nota fora do domínio 1–5 (inclui linhas corrompidas pelo texto livre dos comentários).
# MAGIC - Deduplicação de linhas repetidas pela chave (`id_avaliacao`, `id_pedido`).
# MAGIC - Criação de `possui_comentario` (booleano) para facilitar análises de engajamento.
# MAGIC - Um mesmo pedido pode ter mais de uma avaliação: a regra "considerar a **mais recente**" é aplicada na Gold.

# COMMAND ----------

bronze = spark.table(f"{B}.avaliacoes")
silver = (bronze
    .select(
        F.trim("review_id").alias("id_avaliacao"),
        F.trim("order_id").alias("id_pedido"),
        num("review_score", "INT").alias("nota"),
        F.trim("review_comment_title").alias("titulo_comentario"),
        F.trim("review_comment_message").alias("comentario"),
        ts("review_creation_date").alias("data_criacao"),
        ts("review_answer_timestamp").alias("data_resposta"))
    .filter(F.col("nota").between(1, 5) & F.col("id_pedido").isNotNull())
    .dropDuplicates(["id_avaliacao", "id_pedido"])
    .withColumn("possui_comentario", F.coalesce(F.length("comentario") > 0, F.lit(False))))

gravar(silver, "avaliacoes",
       "Silver | Avaliações de pedidos (nota 1-5). Linhagem: mvp.bronze.avaliacoes.",
       {"id_avaliacao": "Identificador da avaliação (hash). Origem: review_id.",
        "id_pedido": "Pedido avaliado (FK). Origem: order_id.",
        "nota": "Nota de satisfação de 1 (péssimo) a 5 (ótimo). Origem: review_score.",
        "titulo_comentario": "Título opcional do comentário (maioria nula). Origem: review_comment_title.",
        "comentario": "Texto livre opcional do cliente, em português. Origem: review_comment_message.",
        "data_criacao": "Data em que a pesquisa de satisfação foi enviada ao cliente. Origem: review_creation_date.",
        "data_resposta": "Data/hora em que o cliente respondeu. Origem: review_answer_timestamp.",
        "possui_comentario": "TRUE se o cliente escreveu um comentário. Coluna derivada na Silver."},
       bronze.count(), "nota fora de 1-5 descartada, dedup por (id_avaliacao,id_pedido), flag possui_comentario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Produtos (+ tradução de categorias)
# MAGIC - **Join** `LEFT` com `bronze.traducao_categorias` pelo nome da categoria para obter o nome em inglês.
# MAGIC - Produtos **sem categoria** recebem `sem_categoria` (não são descartados, pois têm vendas).
# MAGIC - Categorias sem tradução na fonte mantêm o nome em português na coluna `categoria_en` (em vez de nulo).
# MAGIC - Correção do nome das colunas `*_lenght` (erro de digitação da fonte).

# COMMAND ----------

bronze = spark.table(f"{B}.produtos")
traducao = (spark.table(f"{B}.traducao_categorias")
    .select(F.trim("product_category_name").alias("cat"),
            F.trim("product_category_name_english").alias("cat_en"))
    .dropDuplicates(["cat"]))

silver = (bronze
    .withColumn("cat", F.trim("product_category_name"))
    .join(traducao, on="cat", how="left")
    .select(
        F.trim("product_id").alias("id_produto"),
        F.coalesce(F.col("cat"), F.lit("sem_categoria")).alias("categoria"),
        F.coalesce(F.col("cat_en"), F.col("cat"), F.lit("uncategorized")).alias("categoria_en"),
        num("product_name_lenght", "INT").alias("qtd_caracteres_nome"),
        num("product_description_lenght", "INT").alias("qtd_caracteres_descricao"),
        num("product_photos_qty", "INT").alias("qtd_fotos"),
        num("product_weight_g", "INT").alias("peso_g"),
        num("product_length_cm", "INT").alias("comprimento_cm"),
        num("product_height_cm", "INT").alias("altura_cm"),
        num("product_width_cm", "INT").alias("largura_cm"))
    .filter(F.col("id_produto").isNotNull())
    .dropDuplicates(["id_produto"]))

gravar(silver, "produtos",
       "Silver | Produtos com categoria traduzida. Linhagem: mvp.bronze.produtos LEFT JOIN mvp.bronze.traducao_categorias.",
       {"id_produto": "Chave do produto (hash). Origem: product_id.",
        "categoria": "Categoria em português (73 valores + sem_categoria). Origem: product_category_name.",
        "categoria_en": "Categoria em inglês. Origem: traducao_categorias (fallback: nome em português).",
        "qtd_caracteres_nome": "Tamanho do nome do produto em caracteres. Origem: product_name_lenght.",
        "qtd_caracteres_descricao": "Tamanho da descrição em caracteres. Origem: product_description_lenght.",
        "qtd_fotos": "Quantidade de fotos publicadas no anúncio. Origem: product_photos_qty.",
        "peso_g": "Peso do produto em gramas (0 a ~40.000). Origem: product_weight_g.",
        "comprimento_cm": "Comprimento em cm. Origem: product_length_cm.",
        "altura_cm": "Altura em cm. Origem: product_height_cm.",
        "largura_cm": "Largura em cm. Origem: product_width_cm."},
       bronze.count(), "join com tradução, categoria nula -> sem_categoria, correção de nomes, tipagem INT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Vendedores

# COMMAND ----------

bronze = spark.table(f"{B}.vendedores")
silver = (bronze
    .select(
        F.trim("seller_id").alias("id_vendedor"),
        F.lpad(F.trim("seller_zip_code_prefix"), 5, "0").alias("cep_prefixo"),
        cidade("seller_city").alias("cidade"),
        uf("seller_state").alias("uf"))
    .filter(F.col("id_vendedor").isNotNull())
    .dropDuplicates(["id_vendedor"]))

gravar(silver, "vendedores",
       "Silver | Vendedores parceiros padronizados. Linhagem: mvp.bronze.vendedores.",
       {"id_vendedor": "Chave do vendedor (hash). Origem: seller_id.",
        "cep_prefixo": "5 primeiros dígitos do CEP do vendedor. Origem: seller_zip_code_prefix.",
        "cidade": "Cidade do vendedor em minúsculas e sem acento (a fonte possui ruído de digitação). Origem: seller_city.",
        "uf": "UF do vendedor. Origem: seller_state."},
       bronze.count(), "trim, lpad CEP, padronização cidade/UF, dedup por id_vendedor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Registro do impacto das transformações
# MAGIC Evidência persistida do volume de linhas em cada etapa (útil para auditoria e para a seção de Qualidade de Dados).

# COMMAND ----------

df_controle = spark.createDataFrame(
    controle, "tabela string, linhas_bronze long, linhas_silver long, linhas_descartadas long, transformacoes string"
).withColumn("data_execucao", F.current_timestamp())

df_controle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.controle_carga")
spark.sql(f"COMMENT ON TABLE {S}.controle_carga IS 'Silver | Log de execução: linhas lidas da Bronze, gravadas na Silver e descartadas por tabela.'")
display(df_controle)
