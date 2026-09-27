# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Camada Gold — Modelo dimensional (esquema constelação)
# MAGIC
# MAGIC **Objetivo:** organizar os dados da Silver em um modelo dimensional otimizado para as perguntas de negócio.
# MAGIC
# MAGIC Escolhemos um **esquema constelação** (*galaxy schema*): **duas tabelas fato** que compartilham as mesmas **dimensões conformadas**.
# MAGIC
# MAGIC | Tabela | Tipo | Grão (1 linha =) | Responde a |
# MAGIC |---|---|---|---|
# MAGIC | `fato_pedidos` | Fato | um pedido | logística (prazo, atraso), satisfação (nota), pagamento, recompra |
# MAGIC | `fato_itens_pedido` | Fato | um item vendido dentro de um pedido | receita por categoria, produto, vendedor |
# MAGIC | `dim_tempo` | Dimensão | um dia do calendário | evolução mensal, sazonalidade |
# MAGIC | `dim_cliente` | Dimensão | um `id_cliente` | análise regional (UF/região) e recompra (`id_cliente_unico`) |
# MAGIC | `dim_produto` | Dimensão | um produto | categoria, dimensões físicas |
# MAGIC | `dim_vendedor` | Dimensão | um vendedor | localização do vendedor |
# MAGIC
# MAGIC **Por que duas fatos e não uma?** Porque as métricas vivem em grãos diferentes: preço e frete são do **item**, enquanto prazo de entrega, nota
# MAGIC e pagamento são do **pedido**. Juntar tudo em uma única tabela no grão do item repetiria a nota e o valor pago de um pedido em cada item,
# MAGIC distorcendo médias e somas (problema clássico de *fan-out*).
# MAGIC
# MAGIC **Chaves:** as chaves naturais da Olist já são hashes únicos e estáveis, por isso são usadas diretamente como chaves das dimensões.
# MAGIC A exceção é `dim_tempo`, que usa uma chave substituta inteira no formato `AAAAMMDD` (`sk_data`).
# MAGIC As restrições `PRIMARY KEY`/`FOREIGN KEY` são declaradas no Unity Catalog (informativas), o que permite ao Catalog Explorer desenhar o diagrama de relacionamentos.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "mvp"
S = f"{CATALOG}.silver"
G = f"{CATALOG}.gold"

# As fatos referenciam as dimensões via FOREIGN KEY: removê-las antes de recriar as dimensões
for t in ["fato_pedidos", "fato_itens_pedido"]:
    spark.sql(f"DROP TABLE IF EXISTS {G}.{t}")

REGIOES = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}

def regiao(col_uf):
    expr = None
    for nome, ufs in REGIOES.items():
        cond = F.col(col_uf).isin(ufs)
        expr = F.when(cond, nome) if expr is None else expr.when(cond, nome)
    return expr


def gravar(df, tabela, descricao, comentarios, pk=None):
    nome = f"{G}.{tabela}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome)
    spark.sql(f"COMMENT ON TABLE {nome} IS '{descricao}'")
    for coluna, texto in comentarios.items():
        spark.sql(f"ALTER TABLE {nome} ALTER COLUMN {coluna} COMMENT '{texto}'")
    if pk:
        try:
            spark.sql(f"ALTER TABLE {nome} ALTER COLUMN {pk} SET NOT NULL")
            spark.sql(f"ALTER TABLE {nome} ADD CONSTRAINT pk_{tabela} PRIMARY KEY ({pk})")
        except Exception as e:
            print(f"[aviso] PK não criada em {nome}: {str(e)[:120]}")
    print(f"{nome:30s} {spark.table(nome).count():>10,} linhas")


def fk(tabela, coluna, dim, dim_coluna):
    try:
        spark.sql(f"ALTER TABLE {G}.{tabela} ADD CONSTRAINT fk_{tabela}_{dim} FOREIGN KEY ({coluna}) REFERENCES {G}.{dim}({dim_coluna})")
    except Exception as e:
        print(f"[aviso] FK {tabela}.{coluna} -> {dim} não criada: {str(e)[:120]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 `dim_tempo`
# MAGIC Gerada a partir do intervalo de datas de compra existente na Silver (uma linha por dia, sem lacunas), com atributos de calendário em português.

# COMMAND ----------

lim = spark.table(f"{S}.pedidos").agg(F.min(F.to_date("data_compra")).alias("ini"), F.max(F.to_date("data_compra")).alias("fim")).first()

nomes_mes = F.array(*[F.lit(m) for m in ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]])
nomes_dia = F.array(*[F.lit(d) for d in ["domingo","segunda","terça","quarta","quinta","sexta","sábado"]])

dim_tempo = (spark.sql(f"SELECT explode(sequence(DATE'{lim.ini}', DATE'{lim.fim}', INTERVAL 1 DAY)) AS data")
    .select(
        F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
        "data",
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.element_at(nomes_mes, F.month("data")).alias("nome_mes"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.dayofmonth("data").alias("dia"),
        F.dayofweek("data").alias("dia_semana"),
        F.element_at(nomes_dia, F.dayofweek("data")).alias("nome_dia_semana"),
        F.dayofweek("data").isin(1, 7).alias("flag_fim_de_semana")))

gravar(dim_tempo, "dim_tempo",
       f"Gold | Dimensão calendário, 1 linha por dia de {lim.ini} a {lim.fim}. Linhagem: gerada a partir do intervalo de silver.pedidos.data_compra.",
       {"sk_data": "Chave substituta da data no formato AAAAMMDD (ex.: 20171124). PK.",
        "data": "Data do calendário.",
        "ano": "Ano (2016 a 2018).",
        "trimestre": "Trimestre do ano (1 a 4).",
        "mes": "Mês do ano (1 a 12).",
        "nome_mes": "Nome do mês em português (janeiro..dezembro).",
        "ano_mes": "Ano e mês no formato AAAA-MM, para séries mensais.",
        "dia": "Dia do mês (1 a 31).",
        "dia_semana": "Dia da semana numérico (1 = domingo ... 7 = sábado).",
        "nome_dia_semana": "Nome do dia da semana em português.",
        "flag_fim_de_semana": "TRUE se sábado ou domingo."},
       pk="sk_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 `dim_cliente`, `dim_vendedor` e `dim_produto`
# MAGIC Enriquecimento: derivação da **região geográfica** (IBGE) a partir da UF, e do **volume** do produto a partir de suas dimensões.

# COMMAND ----------

dim_cliente = spark.table(f"{S}.clientes").withColumn("regiao", regiao("uf"))
gravar(dim_cliente, "dim_cliente",
       "Gold | Dimensão cliente (1 linha por id_cliente). Linhagem: mvp.silver.clientes + região derivada da UF.",
       {"id_cliente": "Chave do cliente no pedido (hash). PK. Origem: silver.clientes.id_cliente.",
        "id_cliente_unico": "Identificador da pessoa; um mesmo id_cliente_unico pode ter vários id_cliente (recompra).",
        "cep_prefixo": "5 primeiros dígitos do CEP (00000 a 99999).",
        "cidade": "Cidade do cliente (minúsculas, sem acento).",
        "uf": "UF do cliente (27 siglas).",
        "regiao": "Região do IBGE derivada da UF: Norte, Nordeste, Centro-Oeste, Sudeste, Sul."},
       pk="id_cliente")

dim_vendedor = spark.table(f"{S}.vendedores").withColumn("regiao", regiao("uf"))
gravar(dim_vendedor, "dim_vendedor",
       "Gold | Dimensão vendedor (1 linha por vendedor). Linhagem: mvp.silver.vendedores + região derivada da UF.",
       {"id_vendedor": "Chave do vendedor (hash). PK. Origem: silver.vendedores.id_vendedor.",
        "cep_prefixo": "5 primeiros dígitos do CEP do vendedor.",
        "cidade": "Cidade do vendedor (minúsculas, sem acento; a fonte tem ruído de digitação).",
        "uf": "UF do vendedor.",
        "regiao": "Região do IBGE derivada da UF do vendedor."},
       pk="id_vendedor")

dim_produto = (spark.table(f"{S}.produtos")
    .withColumn("volume_cm3", F.col("comprimento_cm") * F.col("altura_cm") * F.col("largura_cm")))
gravar(dim_produto, "dim_produto",
       "Gold | Dimensão produto (1 linha por produto). Linhagem: mvp.silver.produtos (já traduzida) + volume derivado.",
       {"id_produto": "Chave do produto (hash). PK. Origem: silver.produtos.id_produto.",
        "categoria": "Categoria do produto em português; sem_categoria quando a fonte não informa.",
        "categoria_en": "Categoria do produto em inglês.",
        "qtd_caracteres_nome": "Tamanho do nome do anúncio em caracteres.",
        "qtd_caracteres_descricao": "Tamanho da descrição do anúncio em caracteres.",
        "qtd_fotos": "Quantidade de fotos do anúncio.",
        "peso_g": "Peso em gramas.",
        "comprimento_cm": "Comprimento em cm.",
        "altura_cm": "Altura em cm.",
        "largura_cm": "Largura em cm.",
        "volume_cm3": "Volume em cm³ = comprimento x altura x largura. Coluna derivada na Gold."},
       pk="id_produto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 `fato_itens_pedido` (grão: item)
# MAGIC **Join** de `silver.itens_pedido` com `silver.pedidos` por `id_pedido` para trazer o cliente, a data da compra (chave de tempo) e o status do pedido para cada item.

# COMMAND ----------

pedidos = spark.table(f"{S}.pedidos")

fato_itens = (spark.table(f"{S}.itens_pedido").alias("i")
    .join(pedidos.alias("p"), "id_pedido", "inner")
    .select(
        "id_pedido", "item_seq", "id_produto", "id_vendedor", "p.id_cliente",
        F.date_format("p.data_compra", "yyyyMMdd").cast("int").alias("sk_data_compra"),
        "p.status_pedido",
        "i.preco", "i.frete",
        (F.col("i.preco") + F.col("i.frete")).cast("decimal(12,2)").alias("valor_total_item")))

gravar(fato_itens, "fato_itens_pedido",
       "Gold | Fato de itens vendidos (1 linha por item de pedido). Linhagem: silver.itens_pedido INNER JOIN silver.pedidos (id_pedido).",
       {"id_pedido": "Pedido do item (dimensão degenerada). Parte da chave (id_pedido, item_seq).",
        "item_seq": "Sequência do item no pedido (1..21). Parte da chave.",
        "id_produto": "FK -> dim_produto.",
        "id_vendedor": "FK -> dim_vendedor.",
        "id_cliente": "FK -> dim_cliente. Origem: silver.pedidos.",
        "sk_data_compra": "FK -> dim_tempo (AAAAMMDD da data da compra). Origem: silver.pedidos.data_compra.",
        "status_pedido": "Status do pedido (atributo degenerado): delivered, shipped, canceled, etc.",
        "preco": "Preço do item em R$ (medida aditiva).",
        "frete": "Frete rateado para o item em R$ (medida aditiva).",
        "valor_total_item": "preco + frete, em R$ (medida aditiva derivada)."})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 `fato_pedidos` (grão: pedido)
# MAGIC Consolida em uma linha por pedido:
# MAGIC - **Agregação de itens**: quantidade de itens, de vendedores, valor dos produtos e do frete (`SUM`/`COUNT` agrupados por `id_pedido`).
# MAGIC - **Agregação de pagamentos**: valor pago total, maior número de parcelas e meio de pagamento principal (o de maior valor, via `row_number`).
# MAGIC - **Avaliação**: quando o pedido tem mais de uma avaliação, considera-se a **mais recente** (`row_number` por data de resposta).
# MAGIC - **Métricas logísticas** (somente para pedidos entregues e sem inconsistência de datas):
# MAGIC   - `dias_entrega` = data de entrega − data da compra
# MAGIC   - `dias_prazo_prometido` = data estimada − data da compra
# MAGIC   - `dias_atraso` = data de entrega − data estimada (positivo = atrasou)
# MAGIC   - `flag_atraso` = entregue **em dia posterior** à data prometida (comparação por data, não por hora, pois a data estimada vem sem horário).

# COMMAND ----------

itens_agg = (spark.table(f"{S}.itens_pedido").groupBy("id_pedido").agg(
    F.count("*").alias("qtd_itens"),
    F.countDistinct("id_vendedor").alias("qtd_vendedores"),
    F.sum("preco").cast("decimal(12,2)").alias("valor_produtos"),
    F.sum("frete").cast("decimal(12,2)").alias("valor_frete")))

pag = spark.table(f"{S}.pagamentos")
pag_agg = pag.groupBy("id_pedido").agg(
    F.sum("valor_pagamento").cast("decimal(12,2)").alias("valor_pago"),
    F.max("parcelas").alias("parcelas"),
    F.count("*").alias("qtd_pagamentos"))
w_pag = Window.partitionBy("id_pedido").orderBy(F.col("valor_pagamento").desc(), F.col("pagamento_seq"))
pag_principal = (pag.withColumn("rn", F.row_number().over(w_pag)).filter("rn = 1")
    .select("id_pedido", F.col("tipo_pagamento").alias("tipo_pagamento_principal")))

w_av = Window.partitionBy("id_pedido").orderBy(F.col("data_resposta").desc_nulls_last(), F.col("data_criacao").desc_nulls_last())
aval = (spark.table(f"{S}.avaliacoes").withColumn("rn", F.row_number().over(w_av)).filter("rn = 1")
    .select("id_pedido", F.col("nota").alias("nota_avaliacao"), "possui_comentario"))

entregue_valido = (F.col("status_pedido") == "delivered") & F.col("data_entrega_cliente").isNotNull() & ~F.col("flag_datas_inconsistentes")

fato_pedidos = (pedidos
    .join(itens_agg, "id_pedido", "left")
    .join(pag_agg, "id_pedido", "left")
    .join(pag_principal, "id_pedido", "left")
    .join(aval, "id_pedido", "left")
    .select(
        "id_pedido", "id_cliente",
        F.date_format("data_compra", "yyyyMMdd").cast("int").alias("sk_data_compra"),
        "status_pedido",
        F.coalesce("qtd_itens", F.lit(0)).alias("qtd_itens"),
        F.coalesce("qtd_vendedores", F.lit(0)).alias("qtd_vendedores"),
        "valor_produtos", "valor_frete", "valor_pago",
        "tipo_pagamento_principal", "parcelas", "qtd_pagamentos",
        F.when(entregue_valido, F.datediff(F.to_date("data_entrega_cliente"), F.to_date("data_compra"))).alias("dias_entrega"),
        F.datediff(F.to_date("data_estimada_entrega"), F.to_date("data_compra")).alias("dias_prazo_prometido"),
        F.when(entregue_valido, F.datediff(F.to_date("data_entrega_cliente"), F.to_date("data_estimada_entrega"))).alias("dias_atraso"),
        F.when(entregue_valido, F.to_date("data_entrega_cliente") > F.to_date("data_estimada_entrega")).alias("flag_atraso"),
        "nota_avaliacao", "possui_comentario",
        "flag_datas_inconsistentes"))

gravar(fato_pedidos, "fato_pedidos",
       "Gold | Fato de pedidos (1 linha por pedido) com valores, pagamento, logística e satisfação. Linhagem: silver.pedidos LEFT JOIN agregações de silver.itens_pedido, silver.pagamentos e silver.avaliacoes (id_pedido).",
       {"id_pedido": "Chave do pedido (hash). PK.",
        "id_cliente": "FK -> dim_cliente.",
        "sk_data_compra": "FK -> dim_tempo (AAAAMMDD da data da compra).",
        "status_pedido": "Status do pedido: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved.",
        "qtd_itens": "Quantidade de itens do pedido (0 quando o pedido não tem itens, ex.: cancelado antes do faturamento). SUM de silver.itens_pedido.",
        "qtd_vendedores": "Quantidade de vendedores distintos no pedido.",
        "valor_produtos": "Soma dos preços dos itens em R$ (NULL se o pedido não tem itens).",
        "valor_frete": "Soma do frete dos itens em R$.",
        "valor_pago": "Soma dos pagamentos em R$ (produtos + frete, podendo incluir juros de parcelamento). Origem: silver.pagamentos.",
        "tipo_pagamento_principal": "Meio de pagamento de maior valor no pedido: credit_card, boleto, voucher, debit_card, nao_definido.",
        "parcelas": "Maior número de parcelas entre os pagamentos do pedido (1..24).",
        "qtd_pagamentos": "Quantidade de registros de pagamento do pedido (>1 quando combina meios, ex.: cartão + voucher).",
        "dias_entrega": "Dias corridos entre compra e entrega. Apenas pedidos entregues e com datas consistentes (demais = NULL).",
        "dias_prazo_prometido": "Dias corridos entre a compra e a data estimada de entrega informada ao cliente.",
        "dias_atraso": "Data de entrega - data estimada, em dias. Positivo = atrasou; negativo = chegou antes.",
        "flag_atraso": "TRUE se entregue em data posterior à prometida. NULL para pedidos não entregues.",
        "nota_avaliacao": "Nota da avaliação mais recente do pedido (1 a 5). NULL se não avaliado.",
        "possui_comentario": "TRUE se a avaliação considerada tem comentário escrito.",
        "flag_datas_inconsistentes": "Herdada da Silver: TRUE para cronologia impossível; esses pedidos são excluídos das métricas de prazo."},
       pk="id_pedido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Relacionamentos (chaves estrangeiras informativas)

# COMMAND ----------

try:  # chave primária composta da fato de itens
    spark.sql(f"ALTER TABLE {G}.fato_itens_pedido ALTER COLUMN id_pedido SET NOT NULL")
    spark.sql(f"ALTER TABLE {G}.fato_itens_pedido ALTER COLUMN item_seq SET NOT NULL")
    spark.sql(f"ALTER TABLE {G}.fato_itens_pedido ADD CONSTRAINT pk_fato_itens_pedido PRIMARY KEY (id_pedido, item_seq)")
except Exception as e:
    print(f"[aviso] PK composta não criada: {str(e)[:120]}")

fk("fato_pedidos", "id_cliente", "dim_cliente", "id_cliente")
fk("fato_pedidos", "sk_data_compra", "dim_tempo", "sk_data")
fk("fato_itens_pedido", "id_cliente", "dim_cliente", "id_cliente")
fk("fato_itens_pedido", "sk_data_compra", "dim_tempo", "sk_data")
fk("fato_itens_pedido", "id_produto", "dim_produto", "id_produto")
fk("fato_itens_pedido", "id_vendedor", "dim_vendedor", "id_vendedor")
fk("fato_itens_pedido", "id_pedido", "fato_pedidos", "id_pedido")

display(spark.sql(f"SHOW TABLES IN {G}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Catálogo de dados gerado a partir do Unity Catalog
# MAGIC A consulta abaixo lê o `information_schema` e lista **todas as colunas da Gold com tipo e descrição** — é o catálogo de dados "vivo", mantido junto com as tabelas.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name AS tabela, column_name AS coluna, full_data_type AS tipo, is_nullable AS aceita_nulo, comment AS descricao
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = 'gold'
    ORDER BY table_name, ordinal_position
"""))
