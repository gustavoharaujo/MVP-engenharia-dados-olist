# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Qualidade de Dados
# MAGIC
# MAGIC **Objetivo:** verificar, sobre o dado **bruto** (Bronze), as cinco dimensões de qualidade pedidas no MVP e mostrar como cada problema
# MAGIC encontrado foi tratado na Silver/Gold.
# MAGIC
# MAGIC | Dimensão | Pergunta | Onde |
# MAGIC |---|---|---|
# MAGIC | **Completude** | Existem nulos/vazios? Em que proporção? | 4.1 (perfil de **todos os atributos** de todas as tabelas) |
# MAGIC | **Unicidade** | Existem duplicatas onde não deveria? | 4.2 |
# MAGIC | **Consistência** | Os valores seguem o padrão/domínio esperado? | 4.2 |
# MAGIC | **Acurácia** | Os valores fazem sentido no contexto? | 4.2 |
# MAGIC | **Integridade referencial** | As chaves estrangeiras encontram seu par? | 4.2 |
# MAGIC | **Outliers** | Há valores extremos que distorcem análises? | 4.3 |
# MAGIC
# MAGIC > Ordem de execução: este notebook pode ser executado após o `01_bronze_ingestao` (perfil do bruto) e após o `03_gold_modelagem` (verificação pós-tratamento, seção 4.4).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "mvp"
B = f"{CATALOG}.bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Completude — perfil de todos os atributos da Bronze
# MAGIC Para **cada coluna de cada tabela** calculamos: total de linhas, nulos/vazios, % de completude e quantidade de valores distintos.
# MAGIC O resultado é persistido em `mvp.silver.qualidade_perfil_bronze`.

# COMMAND ----------

perfis = []
tabelas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {B}").collect()]
for t in sorted(tabelas):
    df = spark.table(f"{B}.{t}")
    colunas = [c for c in df.columns if not c.startswith("_")]
    aggs = [F.count(F.lit(1)).alias("__total")]
    for c in colunas:
        aggs.append(F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(f"n__{c}"))
        aggs.append(F.approx_count_distinct(c).alias(f"d__{c}"))
    r = df.agg(*aggs).first()
    total = r["__total"]
    for c in colunas:
        nulos = r[f"n__{c}"]
        perfis.append((t, c, total, nulos, round(100 * (1 - nulos / total), 2), r[f"d__{c}"]))

perfil = spark.createDataFrame(perfis, "tabela string, coluna string, total_linhas long, nulos_ou_vazios long, pct_completude double, distintos_aprox long")
perfil.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.qualidade_perfil_bronze")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.qualidade_perfil_bronze IS 'Qualidade | Perfil de completude e cardinalidade de todas as colunas das tabelas Bronze.'")
display(perfil.orderBy("tabela", "coluna"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apenas as colunas com algum valor faltante

# COMMAND ----------

display(perfil.filter("nulos_ou_vazios > 0").orderBy(F.col("pct_completude")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Unicidade, consistência, acurácia e integridade referencial
# MAGIC Cada verificação é uma consulta SQL que **conta as ocorrências do problema** no dado bruto. Ao lado, o tratamento aplicado no pipeline.

# COMMAND ----------

VERIFICACOES = [
    # ---------------- UNICIDADE ----------------
    ("Unicidade", "pedidos: order_id duplicado",
     f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {B}.pedidos",
     "dropDuplicates(id_pedido) na Silver"),
    ("Unicidade", "clientes: customer_id duplicado",
     f"SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM {B}.clientes",
     "dropDuplicates(id_cliente) na Silver"),
    ("Unicidade", "produtos: product_id duplicado",
     f"SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM {B}.produtos",
     "dropDuplicates(id_produto) na Silver"),
    ("Unicidade", "itens: (order_id, order_item_id) duplicado",
     f"SELECT COUNT(*) - COUNT(DISTINCT order_id, order_item_id) FROM {B}.itens_pedido",
     "dropDuplicates na Silver"),
    ("Unicidade", "avaliações: review_id repetido (mesma avaliação em mais de um pedido)",
     f"SELECT COUNT(*) - COUNT(DISTINCT review_id) FROM {B}.avaliacoes",
     "chave passa a ser (id_avaliacao, id_pedido)"),
    ("Unicidade", "avaliações: pedidos com mais de uma avaliação",
     f"SELECT COUNT(*) FROM (SELECT order_id FROM {B}.avaliacoes GROUP BY order_id HAVING COUNT(*) > 1)",
     "Gold considera apenas a avaliação mais recente"),
    ("Unicidade", "clientes: pessoas (customer_unique_id) com mais de um customer_id",
     f"SELECT COUNT(*) FROM (SELECT customer_unique_id FROM {B}.clientes GROUP BY 1 HAVING COUNT(*) > 1)",
     "esperado (novo id por pedido); usado para medir recompra"),
    # ---------------- CONSISTÊNCIA ----------------
    ("Consistência", "clientes: CEP com menos de 5 dígitos (zero à esquerda perdido)",
     f"SELECT COUNT(*) FROM {B}.clientes WHERE length(trim(customer_zip_code_prefix)) < 5",
     "lpad(cep, 5, '0') na Silver"),
    ("Consistência", "vendedores: CEP com menos de 5 dígitos",
     f"SELECT COUNT(*) FROM {B}.vendedores WHERE length(trim(seller_zip_code_prefix)) < 5",
     "lpad(cep, 5, '0') na Silver"),
    ("Consistência", "clientes/vendedores: UF fora das 27 siglas",
     f"""SELECT (SELECT COUNT(*) FROM {B}.clientes WHERE upper(trim(customer_state)) NOT IN ('AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'))
              + (SELECT COUNT(*) FROM {B}.vendedores WHERE upper(trim(seller_state)) NOT IN ('AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO'))""",
     "UF inválida vira NULL na Silver"),
    ("Consistência", "vendedores: cidade com caracteres fora do padrão (dígitos, /, \\\\, @, -)",
     f"SELECT COUNT(*) FROM {B}.vendedores WHERE seller_city RLIKE '[0-9/@\\\\\\\\-]'",
     "documentado; análises regionais usam UF (confiável)"),
    ("Consistência", "pedidos: datas que não convertem para timestamp",
     f"""SELECT COUNT(*) FROM {B}.pedidos WHERE
           (order_purchase_timestamp IS NOT NULL AND try_to_timestamp(order_purchase_timestamp,'yyyy-MM-dd HH:mm:ss') IS NULL)
        OR (order_delivered_customer_date IS NOT NULL AND try_to_timestamp(order_delivered_customer_date,'yyyy-MM-dd HH:mm:ss') IS NULL)
        OR (order_estimated_delivery_date IS NOT NULL AND try_to_timestamp(order_estimated_delivery_date,'yyyy-MM-dd HH:mm:ss') IS NULL)""",
     "try_to_timestamp (inválido vira NULL)"),
    ("Consistência", "pagamentos: payment_type = not_defined",
     f"SELECT COUNT(*) FROM {B}.pagamentos WHERE payment_type = 'not_defined'",
     "padronizado para nao_definido"),
    ("Consistência", "produtos: categoria sem tradução para inglês",
     f"""SELECT COUNT(DISTINCT p.product_category_name) FROM {B}.produtos p
         LEFT JOIN {B}.traducao_categorias t ON p.product_category_name = t.product_category_name
         WHERE p.product_category_name IS NOT NULL AND t.product_category_name IS NULL""",
     "categoria_en recebe o nome em português"),
    # ---------------- ACURÁCIA ----------------
    ("Acurácia", "itens: preço <= 0",
     f"SELECT COUNT(*) FROM {B}.itens_pedido WHERE try_cast(price AS DOUBLE) <= 0",
     "linhas descartadas na Silver"),
    ("Acurácia", "pagamentos: parcelas = 0",
     f"SELECT COUNT(*) FROM {B}.pagamentos WHERE try_cast(payment_installments AS INT) = 0",
     "corrigido para 1 parcela"),
    ("Acurácia", "pagamentos: valor pago = 0",
     f"SELECT COUNT(*) FROM {B}.pagamentos WHERE try_cast(payment_value AS DOUBLE) = 0",
     "mantido (vouchers de valor zero são válidos)"),
    ("Acurácia", "avaliações: nota fora de 1-5 ou inválida",
     f"SELECT COUNT(*) FROM {B}.avaliacoes WHERE try_cast(review_score AS INT) IS NULL OR try_cast(review_score AS INT) NOT BETWEEN 1 AND 5",
     "linhas descartadas na Silver"),
    ("Acurácia", "pedidos: aprovação antes da compra",
     f"SELECT COUNT(*) FROM {B}.pedidos WHERE to_timestamp(order_approved_at) < to_timestamp(order_purchase_timestamp)",
     "flag_datas_inconsistentes = TRUE"),
    ("Acurácia", "pedidos: entrega ao cliente antes da compra",
     f"SELECT COUNT(*) FROM {B}.pedidos WHERE to_timestamp(order_delivered_customer_date) < to_timestamp(order_purchase_timestamp)",
     "flag_datas_inconsistentes = TRUE"),
    ("Acurácia", "pedidos: entrega à transportadora antes da compra",
     f"SELECT COUNT(*) FROM {B}.pedidos WHERE to_timestamp(order_delivered_carrier_date) < to_timestamp(order_purchase_timestamp)",
     "documentado (não afeta métricas: prazo usa compra e entrega ao cliente)"),
    ("Acurácia", "pedidos: status delivered sem data de entrega",
     f"SELECT COUNT(*) FROM {B}.pedidos WHERE order_status = 'delivered' AND order_delivered_customer_date IS NULL",
     "flag_datas_inconsistentes = TRUE (excluído das métricas de prazo)"),
    ("Acurácia", "pedidos: não entregues porém com data de entrega",
     f"SELECT COUNT(*) FROM {B}.pedidos WHERE order_status <> 'delivered' AND order_delivered_customer_date IS NOT NULL",
     "métricas de prazo só consideram status delivered"),
    ("Acurácia", "produtos: peso = 0 g",
     f"SELECT COUNT(*) FROM {B}.produtos WHERE try_cast(product_weight_g AS INT) = 0",
     "mantido e documentado (peso não é usado nas perguntas)"),
    # ---------------- INTEGRIDADE REFERENCIAL ----------------
    ("Integridade", "itens cujo pedido não existe em pedidos",
     f"SELECT COUNT(*) FROM {B}.itens_pedido i LEFT ANTI JOIN {B}.pedidos p ON i.order_id = p.order_id",
     "INNER JOIN na fato_itens_pedido"),
    ("Integridade", "itens cujo produto não existe em produtos",
     f"SELECT COUNT(*) FROM {B}.itens_pedido i LEFT ANTI JOIN {B}.produtos p ON i.product_id = p.product_id",
     "nenhuma ação necessária se = 0"),
    ("Integridade", "pedidos cujo cliente não existe em clientes",
     f"SELECT COUNT(*) FROM {B}.pedidos o LEFT ANTI JOIN {B}.clientes c ON o.customer_id = c.customer_id",
     "nenhuma ação necessária se = 0"),
    ("Integridade", "pedidos sem nenhum item",
     f"SELECT COUNT(*) FROM {B}.pedidos o LEFT ANTI JOIN {B}.itens_pedido i ON o.order_id = i.order_id",
     "mantidos na fato_pedidos com qtd_itens = 0 (em geral cancelados/indisponíveis)"),
    ("Integridade", "pedidos sem pagamento registrado",
     f"SELECT COUNT(*) FROM {B}.pedidos o LEFT ANTI JOIN {B}.pagamentos p ON o.order_id = p.order_id",
     "valor_pago = NULL na fato_pedidos"),
    ("Integridade", "pedidos sem avaliação",
     f"SELECT COUNT(*) FROM {B}.pedidos o LEFT ANTI JOIN {B}.avaliacoes a ON o.order_id = a.order_id",
     "nota_avaliacao = NULL na fato_pedidos"),
]

resultados = []
for dimensao, verificacao, sql, tratamento in VERIFICACOES:
    qtd = spark.sql(sql).first()[0]
    resultados.append((dimensao, verificacao, int(qtd), tratamento))

df_verif = spark.createDataFrame(resultados, "dimensao string, verificacao string, ocorrencias long, tratamento_no_pipeline string")
df_verif.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.qualidade_verificacoes")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.qualidade_verificacoes IS 'Qualidade | Resultado das verificações de unicidade, consistência, acurácia e integridade sobre a Bronze.'")
display(df_verif)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Outliers
# MAGIC Distribuição das principais medidas numéricas via percentis. Um valor é considerado *outlier* pela regra do intervalo interquartil (IQR):
# MAGIC acima de `Q3 + 1,5 × IQR`.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT 'itens.preco' AS medida, CAST(preco AS DOUBLE) AS v FROM mvp.silver.itens_pedido
# MAGIC   UNION ALL SELECT 'itens.frete', CAST(frete AS DOUBLE) FROM mvp.silver.itens_pedido
# MAGIC   UNION ALL SELECT 'pedidos.valor_pago', CAST(valor_pago AS DOUBLE) FROM mvp.gold.fato_pedidos WHERE valor_pago IS NOT NULL
# MAGIC   UNION ALL SELECT 'pedidos.dias_entrega', CAST(dias_entrega AS DOUBLE) FROM mvp.gold.fato_pedidos WHERE dias_entrega IS NOT NULL
# MAGIC   UNION ALL SELECT 'produtos.peso_g', CAST(peso_g AS DOUBLE) FROM mvp.silver.produtos WHERE peso_g IS NOT NULL
# MAGIC ),
# MAGIC q AS (
# MAGIC   SELECT medida, COUNT(*) AS n, MIN(v) AS minimo,
# MAGIC          percentile_approx(v, 0.25) AS q1, percentile_approx(v, 0.5) AS mediana,
# MAGIC          percentile_approx(v, 0.75) AS q3, percentile_approx(v, 0.99) AS p99, MAX(v) AS maximo
# MAGIC   FROM base GROUP BY medida
# MAGIC )
# MAGIC SELECT q.*, ROUND(q3 + 1.5 * (q3 - q1), 2) AS limite_outlier,
# MAGIC        (SELECT COUNT(*) FROM base b WHERE b.medida = q.medida AND b.v > q.q3 + 1.5 * (q.q3 - q.q1)) AS qtd_outliers,
# MAGIC        ROUND(100 * (SELECT COUNT(*) FROM base b WHERE b.medida = q.medida AND b.v > q.q3 + 1.5 * (q.q3 - q.q1)) / n, 2) AS pct_outliers
# MAGIC FROM q ORDER BY medida

# COMMAND ----------

# MAGIC %md
# MAGIC **Decisão sobre outliers:** os valores extremos de preço, frete e prazo são **legítimos** (produtos caros como relógios e informática,
# MAGIC entregas para regiões remotas). Removê-los distorceria a receita e esconderia justamente os atrasos que queremos estudar.
# MAGIC Por isso **não foram removidos**; nas análises usamos **mediana** junto com a média quando a assimetria é relevante.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Verificação pós-tratamento (Silver/Gold)
# MAGIC Confirma que os problemas tratados não chegam à camada de consumo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fato_pedidos: id_pedido duplicado' AS verificacao, COUNT(*) - COUNT(DISTINCT id_pedido) AS ocorrencias FROM mvp.gold.fato_pedidos
# MAGIC UNION ALL SELECT 'fato_itens_pedido: chave duplicada', COUNT(*) - COUNT(DISTINCT id_pedido, item_seq) FROM mvp.gold.fato_itens_pedido
# MAGIC UNION ALL SELECT 'fato_itens_pedido: preço <= 0', COUNT(*) FROM mvp.gold.fato_itens_pedido WHERE preco <= 0
# MAGIC UNION ALL SELECT 'fato_pedidos: nota fora de 1-5', COUNT(*) FROM mvp.gold.fato_pedidos WHERE nota_avaliacao NOT BETWEEN 1 AND 5
# MAGIC UNION ALL SELECT 'fato_pedidos: parcelas < 1', COUNT(*) FROM mvp.gold.fato_pedidos WHERE parcelas < 1
# MAGIC UNION ALL SELECT 'fato_pedidos: dias_entrega negativo', COUNT(*) FROM mvp.gold.fato_pedidos WHERE dias_entrega < 0
# MAGIC UNION ALL SELECT 'dim_cliente: UF nula', COUNT(*) FROM mvp.gold.dim_cliente WHERE uf IS NULL
# MAGIC UNION ALL SELECT 'dim_produto: categoria nula', COUNT(*) FROM mvp.gold.dim_produto WHERE categoria IS NULL
# MAGIC UNION ALL SELECT 'fato_itens_pedido: produto sem dimensão', COUNT(*) FROM mvp.gold.fato_itens_pedido f LEFT ANTI JOIN mvp.gold.dim_produto d ON f.id_produto = d.id_produto
# MAGIC UNION ALL SELECT 'fato_pedidos: data sem dimensão tempo', COUNT(*) FROM mvp.gold.fato_pedidos f LEFT ANTI JOIN mvp.gold.dim_tempo d ON f.sk_data_compra = d.sk_data
