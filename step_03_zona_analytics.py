# Importações necessárias
from pyspark.sql import SparkSession
import pandas as pd
import sqlite3

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("ZonaAnalytics") \
    .master("local[1]") \
    .getOrCreate()

# Lê o arquivo Parquet usando Spark
df_curated = spark.read.parquet("data/curated/report_2.parquet")

# Seleciona todas as colunas (opcional, pois já está lendo tudo)
df_analytics = df_curated.select("*")

# Exibe os dados e o esquema do DataFrame
df_analytics.show()
df_analytics.printSchema()

# Conta o número total de linhas
total_rows = df_analytics.count()
print("Total df: ", f"{format(total_rows, ',').replace(',', '.')} rows")

# Grava o DataFrame no formato Parquet, modo "append"
df_analytics.write.mode("append").parquet("data/analytics/report_3.parquet")

# Lê o arquivo Parquet usando pandas
df_final = pd.read_parquet("data/analytics/report_3.parquet")

# Conecta ao banco de dados SQLite
conn = sqlite3.connect("github.db")

# Grava o DataFrame no banco SQLite
df_final.to_sql("repos", conn, if_exists="replace", index=False)

# Finaliza a transação e fecha a conexão
conn.commit()
conn.close()

# Encerra a sessão Spark
spark.stop()
