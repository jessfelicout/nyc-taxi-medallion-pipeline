# Databricks notebook source
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Exploração dos Dados Brutos: NYC TLC Trip Records
# MAGIC
# MAGIC Notebook de análise exploratória dos arquivos parquet brutos do NYC TLC, que tem por objetivo entender schema, qualidade, distribuições e anomalias de cada tipo de táxi para embasar as decisões de design da Medallion Architecture.
# MAGIC
# MAGIC **Tipos analisados:** Yellow, Green, FHV (For-Hire Vehicle), FHVHV (High Volume FHV - Uber/Lyft)  
# MAGIC **Período amostrado:** Janeiro a Maio/2023

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

# Configurações
CATALOG = "nyc_taxi"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_SCHEMA = "raw"
VOLUME = "files"
RAW_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
YEARS = [2023]
MONTHS = [1, 2, 3, 4, 5]

print("Configuration:")
print(f"  Raw data path: {RAW_PATH}")
print(f"  Taxi types: {', '.join(TAXI_TYPES)}")
print(f"  Analysis period: {YEARS}, Months {MONTHS}")

# COMMAND ----------

# DBTITLE 1,Create Catalog Schema and Volume
# 1. Create Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"Catalog created: {CATALOG}")

# 2. Create RAW Schema + Volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")

# COMMAND ----------

# DBTITLE 1,Download NYC Trip Data
import os
import urllib.request


def download_parquet(taxi_type: str, year: int, month: int) -> str:
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url      = f"{BASE_URL}/{filename}"
    dest_dir = f"{RAW_PATH}/{taxi_type}"
    dest_path = f"{dest_dir}/{filename}"

    os.makedirs(dest_dir, exist_ok=True)
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, dest_path)
    size_mb = os.path.getsize(dest_path) / 1e6
    print(f"  OK: {size_mb:.1f} MB → {dest_path}")
    return dest_path


def download_all() -> None:
    for taxi_type in TAXI_TYPES:
        for year in YEARS:
            for month in MONTHS:
                try:
                    download_parquet(taxi_type, year, month)
                except Exception as e:
                    print(f"  WARN: {taxi_type} {year}-{month:02d} skipped: {e}")


if __name__ == "__main__":
    download_all()

# COMMAND ----------

# DBTITLE 1,Utilities
def read_raw(taxi_type: str, year: int = 2023, month: int = 1) -> DataFrame:
    """Read raw parquet file for given taxi type and period"""
    path =f"{RAW_PATH}/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
   
    return spark.read.parquet(path)

def null_report(df: DataFrame, name: str):
    """Generate null value report for all columns in a DataFrame"""
    total = df.count()
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
    
    print(f"\n=== Nulos em {name} (total: {total:,}) ===")
    for col, nulls in sorted(null_counts.items(), key=lambda x: -x[1]):
        if nulls > 0:
            print(f"  {col:40s}: {nulls:>8,}  ({100*nulls/total:5.1f}%)")

# COMMAND ----------

# DBTITLE 1,Load Data (Jan/2023)
# Load January 2023 files for all taxi types
yellow = read_raw("yellow")
green  = read_raw("green")
fhv    = read_raw("fhv")
fhvhv  = read_raw("fhvhv")

# COMMAND ----------

# DBTITLE 1,Section 1: Schemas
# MAGIC %md
# MAGIC ## 1. Análise de Schemas
# MAGIC
# MAGIC Quais colunas existem, com quais tipos, e há sobreposição entre os tipos de táxi

# COMMAND ----------

# DBTITLE 1,Yellow Schema
print("=== Yellow ===")
yellow.printSchema()


# COMMAND ----------

# DBTITLE 1,Green Schema
print("=== Green ===")
green.printSchema()

# COMMAND ----------

# DBTITLE 1,FHV Schema
print("=== FHV (For-Hire Vehicle) ===")
fhv.printSchema()

# COMMAND ----------

# DBTITLE 1,FHVHV Schema
print("=== FHVHV (High Volume FHV - Uber, Lyft) ===")
fhvhv.printSchema()

# COMMAND ----------

# DBTITLE 1,Column Overlap Analysis
yellow_cols = set(yellow.columns)
green_cols  = set(green.columns)
fhv_cols    = set(fhv.columns)
fhvhv_cols  = set(fhvhv.columns)

print("=== Colunas exclusivas por tipo ===")
print(f"Só Yellow:  {yellow_cols - green_cols - fhv_cols - fhvhv_cols}")
print(f"Só Green:   {green_cols - yellow_cols - fhv_cols - fhvhv_cols}")
print(f"Só FHV:     {fhv_cols - yellow_cols - green_cols - fhvhv_cols}")
print(f"Só FHVHV:   {fhvhv_cols - yellow_cols - green_cols - fhv_cols}")

print("\n=== Colunas comuns entre Yellow e Green ===")
common_yg = yellow_cols & green_cols
print(f"Total: {len(common_yg)}")
print(sorted(common_yg))

# COMMAND ----------

# DBTITLE 1,Compatibility Note
# MAGIC %md
# MAGIC **Observação:** Yellow e Green possuem `passenger_count` e `total_amount`, colunas esperadas na camada de consumo. Como 
# MAGIC FHV e FHVHV não possuem essas colunas, eles não estarão na camada Gold, apenas nas anteriores. 

# COMMAND ----------

# DBTITLE 1,Section 2: Volume
# MAGIC %md
# MAGIC ## 2. Análise volumétrica

# COMMAND ----------

# DBTITLE 1,Volume January
for name, df in [("yellow", yellow), ("green", green), ("fhv", fhv), ("fhvhv", fhvhv)]:
    print(f"{name:8s}: {df.count():>10,} registros em Jan/2023")

# COMMAND ----------

# DBTITLE 1,Volume All Months
def volume_por_mes(taxi_type: str) -> DataFrame:
    rows = []
    for month in range(1, 6):
        try:
            n = read_raw(taxi_type, month=month).count()
            rows.append((taxi_type, month, n))
        except Exception as e:
            print(f"Erro ao ler {taxi_type} mês {month}: {e}")
            rows.append((taxi_type, month, 0))
    return spark.createDataFrame(rows, ["tipo", "mes", "registros"])

# Calculate volume for all types and months
volume_dfs = [volume_por_mes(tt) for tt in TAXI_TYPES]
volume_total = reduce(lambda a, b: a.union(b), volume_dfs)

volume_total.orderBy("tipo", "mes").show(100)

# COMMAND ----------

# DBTITLE 1,Section 3: Type Consistency
# MAGIC %md
# MAGIC ## 3. Inconsistência de Tipos entre Meses
# MAGIC
# MAGIC Problema conhecido do NYC TLC: o schema pode variar entre meses; colunas que eram `INT` em Janeiro podem ser `DOUBLE` em Maio, por exemplo. 
# MAGIC Isso causa falhas no Delta Lake (`DELTA_FAILED_TO_MERGE_FIELDS`) quando fazemos union de partições com tipos diferentes.

# COMMAND ----------

# DBTITLE 1,Compare Jan vs May
yellow_jan = read_raw("yellow", month=1)
yellow_mai = read_raw("yellow", month=5)

print("=== Colunas com tipo diferente entre Jan e Mai (Yellow) ===")

jan_types = {f.name: str(f.dataType) for f in yellow_jan.schema.fields}
mai_types = {f.name: str(f.dataType) for f in yellow_mai.schema.fields}

for col in jan_types:
    if col in mai_types and jan_types[col] != mai_types[col]:
        print(f"  {col:30s}: Jan={jan_types[col]:15s}  Mai={mai_types[col]:15s}")

if not any(jan_types[c] != mai_types.get(c) for c in jan_types if c in mai_types):
    print("  Nenhuma inconsistência detectada entre Jan e Mai")

# COMMAND ----------

# DBTITLE 1,Type Consistency Conclusion
# MAGIC %md
# MAGIC **Conclusão:** se usarmos schema explícito na Bronze, os meses com tipo diferente vão falhar ou criar tabelas incompatíveis.
# MAGIC
# MAGIC **Solução implementada:** O pipeline usa as capacidades nativas do Delta Lake:
# MAGIC * `mergeSchema=true` na gravação Bronze
# MAGIC * `unionByName(allowMissingColumns=True)` para union de arquivos
# MAGIC * O Delta Lake resolve automaticamente as diferenças de tipo promovendo para o tipo mais genérico (ex: INT → DOUBLE)
# MAGIC
# MAGIC Esta abordagem delega a normalização de tipos ao engine, simplificando o código de ingestão.

# COMMAND ----------

# DBTITLE 1,Section 4: Nulls
# MAGIC %md
# MAGIC ## 4. Análise de Nulos
# MAGIC
# MAGIC Quais colunas têm nulos? Em que proporção? Isso define quais filtros aplicar na Silver ou na Gold.

# COMMAND ----------

# DBTITLE 1,Null Reports
null_report(yellow, "Yellow Jan/2023")
null_report(green,  "Green Jan/2023")
null_report(fhv,   "FHV Jan/2023")
null_report(fhvhv, "FHVHV Jan/2023")

# COMMAND ----------

# DBTITLE 1,Section 5: Date Corruption
# MAGIC %md
# MAGIC ## 5. Datas: Registros Corrompidos

# COMMAND ----------

# DBTITLE 1,Yellow Date Distribution
print("=== Distribuição de anos em tpep_pickup_datetime (Yellow) ===")
(yellow.withColumn("pickup_year", F.year("tpep_pickup_datetime"))
      .groupBy("pickup_year").count()
      .orderBy("pickup_year")
      .show())

# COMMAND ----------

# DBTITLE 1,Green Date Distribution
print("=== Distribuição de anos em lpep_pickup_datetime (Green) ===")
(green.withColumn("pickup_year", F.year("lpep_pickup_datetime"))
     .groupBy("pickup_year").count()
     .orderBy("pickup_year")
     .show(30))

# COMMAND ----------

# DBTITLE 1,Date Corruption Conclusion
# MAGIC %md
# MAGIC **Conclusão:** os registros corrompidos existem mas são residuais (< 0.1%).
# MAGIC São erros de input; não há sentido analítico em processar viagens de 2008 ou 2087.
# MAGIC
# MAGIC **Solução implementada:** Filtro de **date range explícito** na camada **Silver**:
# MAGIC * Configuração centralizada: `PIPELINE_START_DATE` e `PIPELINE_END_DATE` em `config.py`
# MAGIC * Derivados automaticamente de `YEARS` e `MONTHS` (ex: 2023-01-01 a 2023-05-31)
# MAGIC * Predicate pushdown → performance otimizada
# MAGIC * Remove outliers temporais dentro e fora do ano válido
# MAGIC * Regra explícita, testável e documentada no schema da tabela

# COMMAND ----------

# DBTITLE 1,Section 6: Total Amount
# MAGIC %md
# MAGIC ## 6. `total_amount`: Outliers e Valores Negativos

# COMMAND ----------

# DBTITLE 1,Total Amount Statistics
print("=== Estatísticas de total_amount (Yellow) ===")
yellow.select(
    F.min("total_amount").alias("min"),
    F.percentile_approx("total_amount", 0.01).alias("p1"),
    F.percentile_approx("total_amount", 0.25).alias("p25"),
    F.percentile_approx("total_amount", 0.50).alias("mediana"),
    F.percentile_approx("total_amount", 0.75).alias("p75"),
    F.percentile_approx("total_amount", 0.99).alias("p99"),
    F.max("total_amount").alias("max"),
    F.avg("total_amount").alias("media"),
).show()

# COMMAND ----------

# DBTITLE 1,Negative and Zero Amounts
neg = yellow.filter(F.col("total_amount") < 0)
print(f"Registros com total_amount negativo: {neg.count():,}")
neg.select("total_amount", "tpep_pickup_datetime", "payment_type").show(10)

zero = yellow.filter(F.col("total_amount") == 0)
print(f"Registros com total_amount = 0: {zero.count():,}")

# COMMAND ----------

# DBTITLE 1,High Outliers
print("=== Outliers altos (total_amount > 500) ===")
outliers = yellow.filter(F.col("total_amount") > 500)
(outliers
      .select("total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count")
      .orderBy(F.desc("total_amount"))
      .show(10))
print(f"Registros com total_amount > 500: {outliers.count():,}")

# COMMAND ----------

# DBTITLE 1,Total Amount Conclusion
# MAGIC %md
# MAGIC **Conclusão:** `total_amount` apresenta valores negativos (~1,300 registros) que são erros de dados, e zeros que são válidos para `payment_type` 3 (No Charge) e 4 (Dispute).
# MAGIC
# MAGIC **Decisão implementada:** A camada Silver:
# MAGIC * **Filtra negativos:** `total_amount >= 0` (remove valores inválidos)
# MAGIC * **Preserva zeros:** São registros válidos de negócio (viagens sem cobrança ou disputadas)
# MAGIC
# MAGIC Outliers altos (> $500) são raros mas legítimos (viagens longas, múltiplos passageiros, etc.) e são preservados.

# COMMAND ----------

# DBTITLE 1,Section 7: Passenger Count
# MAGIC %md
# MAGIC ## 7. `passenger_count`: Zeros, Nulos e Impacto na Média

# COMMAND ----------

# DBTITLE 1,Passenger Count Distribution
print("=== Distribuição de passenger_count (Yellow) ===")
total = yellow.count()
dist = (yellow.groupBy("passenger_count")
             .count()
             .withColumn("percent", F.round(F.col("count") / total * 100, 2))
             .orderBy("passenger_count"))
display(dist)

# COMMAND ----------

# DBTITLE 1,Passenger Count Impact
total = yellow.count()
problematic = yellow.filter(F.col("passenger_count").isNull() | (F.col("passenger_count") == 0)).count()
print(f"passenger_count nulo ou zero: {problematic:,} de {total:,} ({100*problematic/total:.2f}%)")
print()

avg_com = yellow.agg(F.avg("passenger_count")).collect()[0][0]
avg_sem = yellow.filter(F.col("passenger_count") > 0).agg(F.avg("passenger_count")).collect()[0][0]
print(f"Média com zeros/nulos incluídos: {avg_com:.4f}")
print(f"Média excluindo zeros/nulos:     {avg_sem:.4f}")

# COMMAND ----------

# DBTITLE 1,Passenger Count Conclusion
# MAGIC %md
# MAGIC **Conclusão:** 4.01%% dos registros têm `passenger_count = 0` ou NULL, indicando erro de captura de dados (táxi vazio não é corrida válida).
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 8: Trip Duration
# MAGIC %md
# MAGIC ## 8. Duração das Corridas: Viagens Impossíveis

# COMMAND ----------

# DBTITLE 1,Trip Duration Analysis
yellow_dur = yellow.withColumn(
    "duracao_min",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
)

print("=== Distribuição de duração das corridas (minutos) - Yellow ===")
yellow_dur.select(
    F.min("duracao_min").alias("min"),
    F.percentile_approx("duracao_min", 0.01).alias("p1"),
    F.percentile_approx("duracao_min", 0.50).alias("mediana"),
    F.percentile_approx("duracao_min", 0.99).alias("p99"),
    F.max("duracao_min").alias("max"),
).show()

neg_dur = yellow_dur.filter(F.col("duracao_min") < 0).count()
print(f"Corridas com duração negativa (dropoff antes do pickup): {neg_dur:,}")

# COMMAND ----------

# DBTITLE 1,Trip Duration Conclusion
# MAGIC %md
# MAGIC **Conclusão:** Existem registros com duração negativa (dropoff_datetime < pickup_datetime), indicando corrupção de dados.
# MAGIC
# MAGIC **Solução:** Filtro na camada Silver com validação `dropoff > pickup`, removendo viagens logicamente impossíveis.

# COMMAND ----------

# DBTITLE 1,Section 9: VendorID
# MAGIC %md
# MAGIC ## 9. VendorID Distribution
# MAGIC

# COMMAND ----------

# DBTITLE 1,VendorID Distribution
print("=== VendorID (Yellow) ===")
yellow.groupBy("VendorID").count().orderBy("VendorID").show()

print("=== VendorID (Green) ===")
green.groupBy("VendorID").count().orderBy("VendorID").show()

print("Legenda (TPEP/LPEP):")
print("  1 → Creative Mobile Technologies, LLC")
print("  2 → Curb Mobility, LLC")
print("  6 → Myle Technologies Inc")
print("  7 → Helix (Yellow/TPEP apenas)")

# COMMAND ----------

# DBTITLE 1,Section 11: Summary
# MAGIC %md
# MAGIC ## Resumo das Descobertas
# MAGIC
# MAGIC | Problema encontrado | Impacto | Decisão implementada no pipeline |
# MAGIC |---|---|---|
# MAGIC | Yellow e Green usam nomes diferentes para datas (`tpep_` vs `lpep_`) | Schema não unificável diretamente | **Gold:** Padroniza para `pickup_datetime`/`dropoff_datetime` na criação das tabelas Gold |
# MAGIC | Tipos INT vs DOUBLE variam entre meses (ex: `VendorID`) | Risco de `DELTA_FAILED_TO_MERGE_FIELDS` ao fazer union | **Bronze:** Delta Lake resolve automaticamente via `mergeSchema=true` (promove INT → DOUBLE quando necessário) |
# MAGIC | Registros com `pickup_datetime` em 2008/2087 (< 0.1% do volume) | Outliers temporais distorcem análises | **Silver:** iltro de date range explícito na camada Silver
# MAGIC | `total_amount` negativo | Valores inválidos (~1,300 registros em Jan/2023) | **Silver:** Filtro `total_amount >= 0` aplicado |
# MAGIC | `total_amount = 0` | Comportamento esperado para `payment_type` 3/4 (No charge/Dispute) | **Silver:** Preservado (são registros válidos de negócio) |
# MAGIC | `passenger_count = 0` ou NULL (4% dos registros) | Indica erro de captura | **Silver:** filtro `passenger_count> 0 or passenger_count IS NOT NULL`  
# MAGIC | Duração negativa (dropoff antes de pickup) | Dados corrompidos | **Silver:** Filtro `dropoff > pickup` aplicado |
# MAGIC | `trip_distance = 0` ou outliers (> 500 mi) | Valores irreais | **Silver:** Filtro `0 < trip_distance <= 500` aplicado (yellow/green apenas) |
# MAGIC | FHV não tem `passenger_count` nem `total_amount` | Incompatível com schema Gold de consumo | **Pipeline:** Bronze/Silver apenas; |
# MAGIC | FHVHV usa `base_passenger_fare` em vez de `total_amount` | Requer harmonização não trivial | **Pipeline:** Bronze/Silver apenas; melhoria futura |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema da camada de consumo (Gold)
# MAGIC
# MAGIC | Coluna Gold | Fonte Yellow | Fonte Green | Tipo |
# MAGIC |---|---|---|---|
# MAGIC | `vendor_id` | `VendorID` | `VendorID` | long |
# MAGIC | `passenger_count` | `passenger_count` | `passenger_count` | long |
# MAGIC | `total_amount` | `total_amount` | `total_amount` | double |
# MAGIC | `pickup_datetime` | `tpep_pickup_datetime` | `lpep_pickup_datetime` | timestamp |
# MAGIC | `dropoff_datetime` | `tpep_dropoff_datetime` | `lpep_dropoff_datetime` | timestamp |
# MAGIC | `trip_duration_minutes` | Calculado na Silver | Calculado na Silver | double |
# MAGIC | `pickup_location_id` | `PULocationID` | `PULocationID` | long |
# MAGIC | `dropoff_location_id` | `DOLocationID` | `DOLocationID` | long |
# MAGIC | **Dimensões temporais (Gold)** | | | |
# MAGIC | `year`, `month`, `year_month` | Extraído de pickup_datetime | | int/string |
# MAGIC | `pickup_hour`, `pickup_day_of_week` | Extraído de pickup_datetime | | int |
# MAGIC | `is_weekend` | Derivado (Sat/Sun) | | boolean |
# MAGIC | `avg_speed_mph` | trip_distance / (duration / 60) | | double |