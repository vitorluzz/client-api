print("===============================================")
print("Step 02 - Zona Curated")
print("Iniciando o processo de transformação dos dados...")
print("Importando e iniciando a sessão spark...")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
import os
import time
import shutil
import glob

start_time = time.time()

print("Criando a sessão spark...")
spark = SparkSession.builder \
    .appName("Zona Curated") \
    .master("local[*]") \
    .getOrCreate()

print("Importando o arquivo report_1.parquet...")
df_raw = spark.read.format("parquet").load("data/raw/report_1.parquet")

df_raw.printSchema()
df_raw.show()

print("Começando o tratamento de dados...")

df_curated = df_raw.select(
    F.col("id").cast(T.IntegerType()),
    F.upper(F.split(F.col("full_name"), "/")[0]).alias("organization"),
    F.col("name").cast(T.StringType()),
    F.col("language").cast(T.StringType()),
    F.col("created_at").cast(T.TimestampType()),
    F.col("updated_at").cast(T.TimestampType()),
    F.col("size").cast(T.IntegerType()),
    F.col("stargazers_count").cast(T.IntegerType()),
    F.col("watchers_count").cast(T.IntegerType()),
    F.col("forks_count").cast(T.IntegerType()),
    F.col("open_issues_count").cast(T.IntegerType()),
    F.col("archived").cast(T.BooleanType()),
    F.col("disabled").cast(T.BooleanType()),
    F.col("visibility").cast(T.StringType())
)

# Adiciona contagem por linguagem
window_spec = Window.partitionBy("language")
df_curated = df_curated.withColumn("language_count", F.count("language").over(window_spec).cast(T.IntegerType()))

# Adiciona timestamp de processamento
df_curated = df_curated.withColumn("processed_at", F.current_date())

# Reorganiza colunas com language_count ao lado de language
ordered_columns = [
    "id", "organization", "name", "language", "language_count", "created_at", "updated_at",
    "size", "stargazers_count", "watchers_count", "forks_count", "open_issues_count",
    "archived", "disabled", "visibility", "processed_at"
]
df_curated = df_curated.select(*ordered_columns)

print("Exibindo os dados tratados...")
df_curated.show(truncate=False)
df_curated.printSchema()

# Diretórios
output_path = "data/curated"
final_file_path = os.path.join(output_path, "report_2.parquet")
temp_path = os.path.join(output_path, "_temp")

# Se já existe arquivo final, lê e une
if os.path.exists(final_file_path):
    print("Arquivo existente encontrado. Carregando dados antigos...")
    df_old = spark.read.parquet(final_file_path)
    df_curated = df_old.unionByName(df_curated)

# Remove temp anterior
if os.path.exists(temp_path):
    shutil.rmtree(temp_path)

print("Salvando dados em 1 único arquivo parquet...")
df_curated.coalesce(1).write.mode("overwrite").parquet(temp_path)

# Renomeia o arquivo part-*.parquet para report_2.parquet
part_file = glob.glob(os.path.join(temp_path, "part-*.parquet"))[0]
if os.path.exists(final_file_path):
    os.remove(final_file_path)
shutil.move(part_file, final_file_path)

# Remove diretório temporário
shutil.rmtree(temp_path)

print(f"✔️ Dados salvos com sucesso em: {final_file_path}")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"⏱️ Tempo total de execução: {elapsed_time:.2f} segundos")

spark.stop()
print("===============================================")
