# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Análise de Dados — Respondendo às perguntas de negócio
# MAGIC
# MAGIC Todas as consultas usam **somente a camada Gold** (modelo dimensional). Convenções adotadas:
# MAGIC
# MAGIC - **Receita (GMV)** = soma do preço dos itens (`valor_produtos`), **sem frete**, de pedidos com status diferente de `canceled` e `unavailable`.
# MAGIC - **Período de análise mensal:** jan/2017 a ago/2018, pois os meses de 2016 e set–out/2018 estão incompletos na fonte (poucos pedidos).
# MAGIC - Métricas de prazo consideram apenas pedidos **entregues com datas consistentes** (`dias_entrega IS NOT NULL`).
# MAGIC
# MAGIC > **Problema:** entender o que impulsiona as vendas e a satisfação dos clientes do marketplace Olist, com foco no papel da **logística** (prazo e atraso de entrega).

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Como evoluíram pedidos e receita mês a mês? Existe sazonalidade?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.ano_mes,
# MAGIC        COUNT(*)                                        AS pedidos,
# MAGIC        ROUND(SUM(f.valor_produtos), 2)                 AS receita,
# MAGIC        ROUND(SUM(f.valor_produtos) / COUNT(*), 2)      AS ticket_medio
# MAGIC FROM mvp.gold.fato_pedidos f
# MAGIC JOIN mvp.gold.dim_tempo t ON f.sk_data_compra = t.sk_data
# MAGIC WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
# MAGIC   AND t.data BETWEEN '2017-01-01' AND '2018-08-31'
# MAGIC GROUP BY t.ano_mes
# MAGIC ORDER BY t.ano_mes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dias com mais pedidos no histórico
# MAGIC SELECT t.data, t.nome_dia_semana, COUNT(*) AS pedidos
# MAGIC FROM mvp.gold.fato_pedidos f
# MAGIC JOIN mvp.gold.dim_tempo t ON f.sk_data_compra = t.sk_data
# MAGIC WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
# MAGIC GROUP BY t.data, t.nome_dia_semana
# MAGIC ORDER BY pedidos DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comparação de mesmo período (jan–ago) entre 2017 e 2018
# MAGIC SELECT t.ano, COUNT(*) AS pedidos, ROUND(SUM(f.valor_produtos), 2) AS receita
# MAGIC FROM mvp.gold.fato_pedidos f
# MAGIC JOIN mvp.gold.dim_tempo t ON f.sk_data_compra = t.sk_data
# MAGIC WHERE f.status_pedido NOT IN ('canceled', 'unavailable') AND t.mes BETWEEN 1 AND 8 AND t.ano IN (2017, 2018)
# MAGIC GROUP BY t.ano ORDER BY t.ano

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P1.** A operação cresceu de forma consistente ao longo de 2017: de 787 pedidos (R$ 120 mil) em jan/2017 para 4.547 pedidos em out/2017.
# MAGIC O grande destaque é **novembro/2017**, com 7.423 pedidos e a primeira receita mensal acima de **R$ 1 milhão** (+63% de pedidos em relação a outubro).
# MAGIC O motivo aparece no ranking diário: **24/11/2017 (Black Friday)** teve 1.166 pedidos, mais que o dobro do segundo melhor dia (25/11, com 499), e os quatro dias seguintes também estão no top 5.
# MAGIC Em 2018 a Olist passou a operar em um patamar mais alto e estável (6–7 mil pedidos e R$ 0,85–1 milhão por mês). Comparando o mesmo período (jan–ago),
# MAGIC a receita passou de R$ 3,08 milhões em 2017 para **R$ 7,34 milhões em 2018 (+138%)**. O **ticket médio** ficou estável, entre R$ 125 e R$ 153:
# MAGIC o crescimento veio do **volume de pedidos**, e não do aumento do valor de cada compra.
# MAGIC Implicação: a capacidade logística deve ser dimensionada para picos sazonais — veremos em P4 que o pico de novembro coincidiu com um salto na taxa de atraso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Quais categorias concentram a receita? Qual o preço médio delas?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.categoria,
# MAGIC        ROUND(SUM(f.preco), 2)                                      AS receita,
# MAGIC        ROUND(100 * SUM(f.preco) / SUM(SUM(f.preco)) OVER (), 2)    AS pct_receita,
# MAGIC        COUNT(*)                                                    AS itens_vendidos,
# MAGIC        ROUND(AVG(f.preco), 2)                                      AS preco_medio_item,
# MAGIC        COUNT(DISTINCT f.id_pedido)                                 AS pedidos
# MAGIC FROM mvp.gold.fato_itens_pedido f
# MAGIC JOIN mvp.gold.dim_produto p ON f.id_produto = p.id_produto
# MAGIC WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
# MAGIC GROUP BY p.categoria
# MAGIC ORDER BY receita DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P2.** Das 74 categorias (73 + `sem_categoria`), as **10 maiores concentram 62,4% da receita** de R$ 13,49 milhões.
# MAGIC A liderança é de **beleza_saude** (R$ 1,26 mi; 9,3%), seguida de perto por **relogios_presentes** (R$ 1,20 mi; 8,9%).
# MAGIC As duas chegam lá por caminhos diferentes: **cama_mesa_banho** é a categoria com **mais itens vendidos** (11.097), mas com preço médio baixo
# MAGIC (R$ 93), enquanto relógios vende quase metade dos itens (5.970) com o **maior preço médio do top 10 (R$ 201)**.
# MAGIC Ou seja, há dois perfis de categoria relevantes: **volume** (cama/mesa/banho, móveis, utilidades domésticas) e **valor** (relógios, *cool stuff*, automotivo).
# MAGIC Para o negócio, isso sugere estratégias distintas: frete e logística eficientes para as de volume, e ações de conversão e parcelamento para as de alto valor.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Como pedidos, frete, prazo e atraso variam entre as regiões/UFs?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.regiao,
# MAGIC        COUNT(*)                                                         AS pedidos,
# MAGIC        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                 AS pct_pedidos,
# MAGIC        ROUND(100 * SUM(f.valor_frete) / SUM(f.valor_produtos), 2)       AS frete_pct_do_valor,
# MAGIC        ROUND(AVG(f.dias_entrega), 1)                                    AS dias_entrega_medio,
# MAGIC        ROUND(100 * AVG(CAST(f.flag_atraso AS INT)), 2)                  AS taxa_atraso_pct,
# MAGIC        ROUND(AVG(f.nota_avaliacao), 2)                                  AS nota_media
# MAGIC FROM mvp.gold.fato_pedidos f
# MAGIC JOIN mvp.gold.dim_cliente c ON f.id_cliente = c.id_cliente
# MAGIC WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
# MAGIC GROUP BY c.regiao
# MAGIC ORDER BY pedidos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.uf,
# MAGIC        COUNT(*)                                                         AS pedidos,
# MAGIC        ROUND(100 * SUM(f.valor_frete) / SUM(f.valor_produtos), 2)       AS frete_pct_do_valor,
# MAGIC        ROUND(AVG(f.dias_entrega), 1)                                    AS dias_entrega_medio,
# MAGIC        percentile_approx(f.dias_entrega, 0.5)                           AS dias_entrega_mediana,
# MAGIC        ROUND(100 * AVG(CAST(f.flag_atraso AS INT)), 2)                  AS taxa_atraso_pct,
# MAGIC        ROUND(AVG(f.nota_avaliacao), 2)                                  AS nota_media
# MAGIC FROM mvp.gold.fato_pedidos f
# MAGIC JOIN mvp.gold.dim_cliente c ON f.id_cliente = c.id_cliente
# MAGIC WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
# MAGIC GROUP BY c.uf
# MAGIC ORDER BY pedidos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- De onde saem as mercadorias? (UF do vendedor, por itens vendidos)
# MAGIC SELECT v.uf AS uf_vendedor, COUNT(*) AS itens, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_itens
# MAGIC FROM mvp.gold.fato_itens_pedido f
# MAGIC JOIN mvp.gold.dim_vendedor v ON f.id_vendedor = v.id_vendedor
# MAGIC GROUP BY v.uf ORDER BY itens DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P3.** A demanda é fortemente concentrada no **Sudeste (68,6% dos pedidos)**; só **SP responde por 41,9%**. A oferta é ainda mais concentrada:
# MAGIC **71,3% dos itens são vendidos por vendedores de SP**. Essa geografia explica o resto da tabela: quanto mais longe de SP, maior o prazo e o custo relativo do frete.
# MAGIC
# MAGIC | Região | % pedidos | Frete / valor | Dias de entrega (média) | Taxa de atraso | Nota média |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Sudeste | 68,6% | 15,2% | 10,7 | 6,1% | 4,14 |
# MAGIC | Sul | 14,3% | 17,6% | 14,0 | 5,9% | 4,16 |
# MAGIC | Nordeste | 9,5% | 21,7% | 19,9 | **12,7%** | **3,92** |
# MAGIC | Centro-Oeste | 5,8% | 17,6% | 15,0 | 6,5% | 4,09 |
# MAGIC | Norte | 1,9% | **22,7%** | **22,5** | 8,6% | 3,98 |
# MAGIC
# MAGIC Um cliente de SP recebe em ~8,7 dias em média (mediana 7), enquanto em RR são 29,3 dias e no AP 27,2. No Norte e Nordeste o frete pesa **mais de 21% do valor dos produtos**,
# MAGIC contra 13,8% em SP (em RR chega a 28,6%). O dado mais relevante para o negócio é o **Nordeste**: tem a **maior taxa de atraso (12,7%)** e a **menor nota média (3,92)**,
# MAGIC com casos críticos em **AL (21,4% de atraso)**, **MA (17,4%)** e **SE (15,2%)**. Curiosamente, o Norte tem prazos maiores, mas taxa de atraso menor (8,6%),
# MAGIC o que indica que ali a **promessa de prazo é mais conservadora**. Já o **RJ** foge ao padrão do Sudeste: 12,1% de atraso e nota 3,90, bem pior que SP (4,5%; 4,21) e MG (4,6%; 4,16),
# MAGIC apesar da proximidade. Isso sugere um problema logístico específico do estado, e não apenas de distância.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · O atraso na entrega impacta a nota de avaliação do cliente?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN flag_atraso THEN 'Entregue com atraso' ELSE 'Entregue no prazo' END AS situacao,
# MAGIC        COUNT(*)                                                        AS pedidos,
# MAGIC        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                AS pct_pedidos,
# MAGIC        ROUND(AVG(nota_avaliacao), 2)                                   AS nota_media,
# MAGIC        ROUND(100 * AVG(CASE WHEN nota_avaliacao <= 2 THEN 1 ELSE 0 END), 1) AS pct_notas_1_ou_2,
# MAGIC        ROUND(100 * AVG(CASE WHEN nota_avaliacao = 5 THEN 1 ELSE 0 END), 1)  AS pct_nota_5
# MAGIC FROM mvp.gold.fato_pedidos
# MAGIC WHERE flag_atraso IS NOT NULL AND nota_avaliacao IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nota por faixa de antecedência/atraso em relação à data prometida
# MAGIC SELECT CASE WHEN dias_atraso < -10 THEN '1. adiantado > 10 dias'
# MAGIC             WHEN dias_atraso < 0   THEN '2. adiantado 1 a 10 dias'
# MAGIC             WHEN dias_atraso = 0   THEN '3. no dia prometido'
# MAGIC             WHEN dias_atraso <= 3  THEN '4. atraso 1 a 3 dias'
# MAGIC             WHEN dias_atraso <= 7  THEN '5. atraso 4 a 7 dias'
# MAGIC             WHEN dias_atraso <= 14 THEN '6. atraso 8 a 14 dias'
# MAGIC             ELSE                        '7. atraso > 14 dias' END              AS faixa,
# MAGIC        COUNT(*)                                                             AS pedidos,
# MAGIC        ROUND(AVG(nota_avaliacao), 2)                                        AS nota_media,
# MAGIC        ROUND(100 * AVG(CASE WHEN nota_avaliacao <= 2 THEN 1 ELSE 0 END), 1) AS pct_notas_1_ou_2
# MAGIC FROM mvp.gold.fato_pedidos
# MAGIC WHERE dias_atraso IS NOT NULL AND nota_avaliacao IS NOT NULL
# MAGIC GROUP BY 1 ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Resumo do desempenho logístico e correlação entre dias de atraso e nota
# MAGIC SELECT ROUND(100 * AVG(CAST(flag_atraso AS INT)), 2)            AS taxa_atraso_pct,
# MAGIC        ROUND(AVG(dias_entrega), 1)                              AS dias_entrega_medio,
# MAGIC        percentile_approx(dias_entrega, 0.5)                     AS dias_entrega_mediana,
# MAGIC        ROUND(AVG(dias_prazo_prometido), 1)                      AS prazo_prometido_medio,
# MAGIC        ROUND(corr(dias_atraso, nota_avaliacao), 3)              AS correlacao_atraso_nota
# MAGIC FROM mvp.gold.fato_pedidos
# MAGIC WHERE dias_entrega IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taxa de atraso por mês (conecta com o pico de vendas da P1)
# MAGIC SELECT t.ano_mes, COUNT(*) AS pedidos_entregues, ROUND(100 * AVG(CAST(f.flag_atraso AS INT)), 1) AS taxa_atraso_pct
# MAGIC FROM mvp.gold.fato_pedidos f JOIN mvp.gold.dim_tempo t ON f.sk_data_compra = t.sk_data
# MAGIC WHERE f.flag_atraso IS NOT NULL AND t.data BETWEEN '2017-01-01' AND '2018-08-31'
# MAGIC GROUP BY t.ano_mes ORDER BY t.ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P4 — a resposta é sim, e o impacto é forte.** Dos 95.824 pedidos entregues e avaliados, **6,7% chegaram depois da data prometida**.
# MAGIC A nota média desses pedidos é **2,27**, contra **4,29** nos entregues no prazo. A proporção de notas 1 ou 2 salta de **9,3% para 62,4%**,
# MAGIC e a de nota 5 cai de 62,3% para 16,5%.
# MAGIC
# MAGIC A análise por faixas mostra um efeito **dose-resposta**: basta 1 a 3 dias de atraso para a nota cair a 3,29 (32% de notas ruins). Com 4 a 7 dias, ela já está em 2,10,
# MAGIC e acima de 8 dias se estabiliza perto de 1,7 (cerca de 80% de notas ruins). Do outro lado, entregar **muito antes** do prometido (mais de 10 dias) gera a melhor nota (4,32).
# MAGIC Isso só é possível porque a Olist promete em média **24 dias**, enquanto a entrega real leva em média **12,5 dias** (mediana 10). Há uma "folga" proposital no prazo prometido.
# MAGIC
# MAGIC A correlação linear entre dias de atraso e nota é **−0,27**. É moderada porque a maioria dos pedidos chega adiantada e ali a nota varia por outros motivos (produto, atendimento).
# MAGIC O efeito, portanto, **não é linear**: o que destrói a satisfação é **cruzar a data prometida**.
# MAGIC
# MAGIC A série mensal liga P1 e P4: a taxa de atraso, que girava em torno de 3–5% em 2017, saltou para **12,4% em nov/2017** (Black Friday) e para **14,1% e 19,0% em fev e mar/2018**.
# MAGIC Nos picos de demanda, a logística não acompanhou.
# MAGIC
# MAGIC *Ressalva:* é uma associação observacional (sem experimento controlado). Ainda assim, a diferença é grande, consistente em todas as faixas e plausível do ponto de vista de negócio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Quais meios de pagamento predominam e como o parcelamento se relaciona com o valor do pedido?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo_pagamento_principal,
# MAGIC        COUNT(*)                                                  AS pedidos,
# MAGIC        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_pedidos,
# MAGIC        ROUND(100 * SUM(valor_pago) / SUM(SUM(valor_pago)) OVER (), 2) AS pct_valor,
# MAGIC        ROUND(AVG(valor_pago), 2)                                 AS ticket_medio_pago,
# MAGIC        ROUND(AVG(parcelas), 2)                                   AS parcelas_media
# MAGIC FROM mvp.gold.fato_pedidos
# MAGIC WHERE status_pedido NOT IN ('canceled', 'unavailable') AND tipo_pagamento_principal IS NOT NULL
# MAGIC GROUP BY tipo_pagamento_principal
# MAGIC ORDER BY pedidos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cartão de crédito: parcelamento por faixa de valor do pedido
# MAGIC SELECT CASE WHEN valor_pago <= 50   THEN '1. até R$ 50'
# MAGIC             WHEN valor_pago <= 100  THEN '2. R$ 50 a 100'
# MAGIC             WHEN valor_pago <= 200  THEN '3. R$ 100 a 200'
# MAGIC             WHEN valor_pago <= 500  THEN '4. R$ 200 a 500'
# MAGIC             WHEN valor_pago <= 1000 THEN '5. R$ 500 a 1.000'
# MAGIC             ELSE                         '6. acima de R$ 1.000' END   AS faixa_valor,
# MAGIC        COUNT(*)                                                      AS pedidos,
# MAGIC        ROUND(AVG(parcelas), 2)                                       AS parcelas_media,
# MAGIC        ROUND(100 * AVG(CASE WHEN parcelas = 1 THEN 1 ELSE 0 END), 1) AS pct_a_vista
# MAGIC FROM mvp.gold.fato_pedidos
# MAGIC WHERE tipo_pagamento_principal = 'credit_card' AND valor_pago > 0
# MAGIC GROUP BY 1 ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P5.** O **cartão de crédito domina**: é o meio principal em **75,5% dos pedidos** válidos e responde por **78,5% do valor pago**.
# MAGIC O **boleto** vem em segundo (19,9% dos pedidos). Somados, os dois cobrem mais de 95% das compras. Voucher (3,1%) e débito (1,5%) são marginais.
# MAGIC O ticket médio no cartão (R$ 167) é maior que no boleto (R$ 145), coerente com a possibilidade de parcelar.
# MAGIC
# MAGIC Dentro do cartão, o **parcelamento cresce claramente com o valor**. Em pedidos de até R$ 50, 60% são à vista (média de 1,75 parcela).
# MAGIC Entre R$ 200 e R$ 500 a média sobe para 5,2 parcelas, e acima de R$ 1.000 para 7,8 parcelas, com apenas 7,7% à vista
# MAGIC (correlação valor × parcelas de +0,37). Chama atenção que **mesmo compras baixas são parceladas**: 57% dos pedidos entre R$ 50 e R$ 100 usam mais de uma parcela.
# MAGIC O parcelamento sem juros funciona como ferramenta de conversão. Para as categorias de alto valor da P2 (relógios, informática), ele deve ser tratado como alavanca comercial.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Qual a taxa de recompra? Uma experiência ruim na 1ª compra reduz a chance de o cliente voltar?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taxa de recompra: pessoas (id_cliente_unico) com mais de um pedido
# MAGIC WITH por_pessoa AS (
# MAGIC   SELECT c.id_cliente_unico, COUNT(DISTINCT f.id_pedido) AS qtd_pedidos
# MAGIC   FROM mvp.gold.fato_pedidos f JOIN mvp.gold.dim_cliente c ON f.id_cliente = c.id_cliente
# MAGIC   GROUP BY c.id_cliente_unico
# MAGIC )
# MAGIC SELECT COUNT(*)                                                         AS clientes_unicos,
# MAGIC        SUM(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END)                 AS clientes_com_recompra,
# MAGIC        ROUND(100 * AVG(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END), 2) AS taxa_recompra_pct,
# MAGIC        MAX(qtd_pedidos)                                                 AS max_pedidos_por_cliente
# MAGIC FROM por_pessoa

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recompra conforme a experiência na PRIMEIRA compra (1ªs compras até mai/2018, para dar tempo de o cliente voltar)
# MAGIC WITH pedidos_pessoa AS (
# MAGIC   SELECT c.id_cliente_unico, f.id_pedido, f.flag_atraso, f.nota_avaliacao, t.data,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY c.id_cliente_unico ORDER BY t.data, f.id_pedido) AS ordem,
# MAGIC          COUNT(*)     OVER (PARTITION BY c.id_cliente_unico)                               AS qtd_pedidos
# MAGIC   FROM mvp.gold.fato_pedidos f
# MAGIC   JOIN mvp.gold.dim_cliente c ON f.id_cliente = c.id_cliente
# MAGIC   JOIN mvp.gold.dim_tempo  t ON f.sk_data_compra = t.sk_data
# MAGIC ),
# MAGIC primeira AS (SELECT * FROM pedidos_pessoa WHERE ordem = 1 AND data < '2018-06-01')
# MAGIC SELECT 'atraso na 1ª compra' AS recorte, CAST(flag_atraso AS STRING) AS valor, COUNT(*) AS clientes,
# MAGIC        ROUND(100 * AVG(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END), 2) AS taxa_recompra_pct
# MAGIC FROM primeira WHERE flag_atraso IS NOT NULL GROUP BY flag_atraso
# MAGIC UNION ALL
# MAGIC SELECT 'nota da 1ª compra', CAST(nota_avaliacao AS STRING), COUNT(*),
# MAGIC        ROUND(100 * AVG(CASE WHEN qtd_pedidos > 1 THEN 1 ELSE 0 END), 2)
# MAGIC FROM primeira WHERE nota_avaliacao IS NOT NULL GROUP BY nota_avaliacao
# MAGIC ORDER BY recorte, valor

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P6 — respondida apenas parcialmente.** Dos **96.096 clientes únicos**, somente **2.997 (3,12%) compraram mais de uma vez** (máximo: 17 pedidos de uma mesma pessoa).
# MAGIC A Olist é, na prática, um canal de **compra única**, e a aquisição de novos clientes, não a retenção, explica o crescimento visto em P1.
# MAGIC
# MAGIC Sobre o efeito da experiência: clientes cuja **1ª compra atrasou voltaram menos (2,68%)** do que os que receberam no prazo (**3,61%**), uma redução relativa de ~26%.
# MAGIC Quem deu **nota 5** na 1ª compra voltou mais (3,76%) do que quem deu nota 1 (3,36%). A direção confirma a hipótese, mas **as diferenças são pequenas em pontos absolutos**
# MAGIC e a base de recompra é muito reduzida. Por isso **não é possível afirmar com segurança uma relação de causa**.
# MAGIC
# MAGIC Limitações que impedem uma resposta conclusiva:
# MAGIC 1. a janela de observação é curta (~2 anos);
# MAGIC 2. o dataset só enxerga compras **via Olist**: o cliente pode ter voltado a comprar do mesmo vendedor em outro canal;
# MAGIC 3. não há dados de marketing/campanhas que expliquem o retorno.
# MAGIC
# MAGIC Esta pergunta permanece no objetivo, conforme orientação do MVP, e é retomada na autoavaliação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC As respostas se conectam em uma narrativa única sobre o problema proposto:
# MAGIC
# MAGIC 1. **O crescimento veio de volume** (P1): mais pedidos, ticket estável, forte sazonalidade na Black Friday.
# MAGIC 2. **A receita é concentrada** em poucas categorias (P2) e em uma geografia (P3): Sudeste compra, SP vende.
# MAGIC 3. **A distância de SP define custo e prazo** (P3): Norte e Nordeste pagam proporcionalmente mais frete, esperam mais e sofrem mais atrasos.
# MAGIC 4. **O atraso é o principal destruidor de satisfação** (P4): cruzar a data prometida derruba a nota de 4,29 para 2,27. Os picos de demanda elevaram o atraso.
# MAGIC 5. **O cliente paga em cartão e parcela** (P5), inclusive compras baixas.
# MAGIC 6. **Quase ninguém volta** (P6), e há indícios, não conclusivos, de que uma má primeira experiência reduz ainda mais o retorno.
# MAGIC
# MAGIC **Recomendação de negócio:** priorizar a confiabilidade do prazo prometido, em especial no **Nordeste e no RJ** e nos **picos sazonais**, e manter a margem de segurança
# MAGIC no prazo informado. Como a satisfação cai de forma abrupta ao cruzar a data prometida, **cumprir a promessa** vale mais do que simplesmente entregar mais rápido.
