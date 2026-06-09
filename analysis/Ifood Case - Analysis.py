# Databricks notebook source
# DBTITLE 1,Configuration and Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "nyc_taxi"
GOLD_SCHEMA = "gold"

yellow_trips_table = f"{CATALOG}.{GOLD_SCHEMA}.yellow_trips_gold"
green_trips_table = f"{CATALOG}.{GOLD_SCHEMA}.green_trips_gold"

# COMMAND ----------

# DBTITLE 1,Question 1
# MAGIC %md
# MAGIC ### Média do valor total `total_amount` recebido em um mês para todos os yellow táxis da frota

# COMMAND ----------

# DBTITLE 1,AVG_TOTAL_AMOUNT Yellow Taxis
df = spark.table(yellow_trips_table)
df_monthly_avg = df \
    .groupBy("year_month") \
    .agg(F.round(F.avg("total_amount"), 2).alias("AVG_TOTAL_AMOUNT")) \
    .orderBy("year_month")

print("\nMédia do valor total por mês - NYC Yellow Taxis \n")
display(df_monthly_avg)

# COMMAND ----------

# DBTITLE 1,Question 2
# MAGIC %md
# MAGIC ### Média de passageiros `passenger_count` por cada hora do dia que pegaram táxi no mês de maio,considerando todos os táxis da frota

# COMMAND ----------

# MAGIC %md
# MAGIC > *Interpretação A*: Qual a média da quantidade passageiros (volume) por hora para determinado mês?

# COMMAND ----------

df_all_taxis = spark.table(yellow_trips_table).union(spark.table(green_trips_table))

# Step 1: Aggregate total passengers by (day, hour)
df_daily_hourly = df_all_taxis \
    .filter(F.col("year_month") == "2023-05") \
    .withColumn("day_of_month", F.dayofmonth("pickup_datetime")) \
    .groupBy("day_of_month", "pickup_hour") \
    .agg(F.sum("passenger_count").alias("total_passengers_that_day_hour"))

# Step 2: Calculate average across days for each hour
df_daily_avg = df_daily_hourly \
    .groupBy("pickup_hour") \
    .agg(F.round(F.avg("total_passengers_that_day_hour"), 2).alias("AVG_TOTAL_PASSENGERS")) \
    .orderBy("pickup_hour")


print("\nMédia de passageiros por hora do dia para o mês de Maio:\n")

display(df_daily_avg)


# COMMAND ----------

# MAGIC %md
# MAGIC > *Interpretação B*: Qual é a média de ocupação do veículo (passageiros por viagem) por hora?

# COMMAND ----------

df_all_taxis.filter(F.col("year_month") == "2023-05") \
    .groupBy("pickup_hour") \
    .agg(F.round(F.avg("passenger_count"), 2).alias("AVG_PASSENGER_COUNT")) \
    .orderBy("pickup_hour")

print("\nMédia da ocupação de passageiros por hora do dia para o mês de Maio:\n")

display(df_all_taxis)

# COMMAND ----------

# DBTITLE 1,Comparison of Interpretations
# MAGIC %md
# MAGIC ## Comparação das Interpretações
# MAGIC
# MAGIC
# MAGIC **Interpretação A (Volume Total)**
# MAGIC * Valores altos indicam horários de pico de demanda
# MAGIC * Picos esperados: Manhã (7-9h), Fim de tarde (17-19h)
# MAGIC * Baixos esperados: Madrugada (3-5h)
# MAGIC * Grande variação entre pico e vale
# MAGIC
# MAGIC **Interpretação B (Ocupação)**
# MAGIC * Valores próximos de 1,0 indicam viagens individuais
# MAGIC * Valores > 1,5 indicam mais viagens em grupo/família
# MAGIC * Variação esperada: Pequena (tipicamente 1,2-1,6)
# MAGIC * Padrão pode ser mais uniforme ao longo do dia
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quando Usar Cada Uma
# MAGIC
# MAGIC **Use a Interpretação A quando:**
# MAGIC * Dimensionar a capacidade da frota
# MAGIC * Prever demanda futura
# MAGIC * Planejar recursos operacionais
# MAGIC * Pergunta: "Quantos veículos preciso neste horário?"
# MAGIC
# MAGIC **Use a Interpretação B quando:**
# MAGIC * Avaliar a eficiência de utilização dos veículos
# MAGIC * Desenvolver estratégias de ride-sharing
# MAGIC * Analisar padrões de comportamento dos clientes
# MAGIC * Pergunta: "Os veículos estão sendo bem utilizados?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Nota:** Ambas as interpretações são válidas e complementares. A escolha depende do objetivo de negócio.