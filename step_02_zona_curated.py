# // salvar o arquivo na curated como parquet respeitando os tipos de daos
# // tipo datas, tirar o nome microsoft do fullname etc

# // trocar o nome do campo de fullname para organization e colocar o texto em letras maiusculas
# // id int,full_name string,name string,language string,created_at datetime,updated_at datetime
print("===============================================")
print("Step 02 - Zona Curated")
print("Importando e iniciando a sessão spark...")
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
 
print("Criando a sessão spark...")
spark = SparkSession.builder \
    .appName("Zona Curated") \
    .master("local[*]") \
    .getOrCreate()
 
print("Importando o arquivo zona_raw.csv...")
df_raw=spark.read.format("com.databricks.spark.csv") \
.option("header", "true") \
.option("delimiter", ",") \
.option("inferSchema", "true") \
.load("raw/microsoft_repos.csv")

df_raw.printSchema()

print("Exibindo os dados...")
df_raw.show()

"""
FORMA ERRADA: (MAL OTIMIZADA!!!!)
Não existe forma certa ou errada, mas sim formas mais otimizadas e menos otimizadas de se fazer a mesma coisa.
Aqui, estamos fazendo um tratamento de dados de forma sequencial, ou seja, um após o outro.
Isso é ruim, pois o spark irá executar cada comando de forma sequencial, ou seja, um após o outro.
Sem contar que a abertura de diversos dataframes intermediários é ruim para a performance do spark.
Vamos supor que a base de dados seja de 5gb, o spark terá que abrir 5gb para cada dataframe intermediário, 
só no contexto abaixo, teríamos 3 dataframes intermediários, ou seja, 15gb de dados abertos.
O correto seria fazer tudo em um único comando, para que o spark possa otimizar a execução dos comandos.


print("Começando o tratamento de dados...")

print("tratando os dados dentro da coluna full_name, truncando depois da barra...")
df2 = df.withColumn("full_name", split(df["full_name"], "/")[0]) 

print("Renomeando a coluna full_name para organization...")
df3 = df2.withColumnRenamed("full_name", "organization");

print("Deixando todo conteúdo da coluna organization em caixa alta...")
df4 = df3.withColumn("organization", upper(df3["organization"]))

print("Exibindo os dados tratados...")
df4.show()
print("Exibindo o squema tratado...")
df4.printSchema()

"""

print("Começando o tratamento de dados...")
print("tratando os dados dentro da coluna full_name, truncando depois da barra...")
print("Renomeando a coluna full_name para organization...")
print("Deixando todo conteúdo da coluna organization em caixa alta...")
df_curated = df_raw.select(
    F.col("id").cast(T.IntegerType()),
    F.upper(F.split(F.col("full_name"), "/")[0]).alias("organization"),
    F.col("name").cast(T.StringType()),
    F.col("language").cast(T.StringType()),
    F.col("created_at").cast(T.TimestampType()),
    F.col("updated_at").cast(T.TimestampType())
)
print("Tratamento de dados foi um sucesso!")

print("Exibindo os dados tratados...")
df_curated.show(truncate=False)
print("Exibindo o squema tratado...")
df_curated.printSchema()

print("Salvando os dados tratados na zona curated...")
print("OBS: se já existir um arquivo parquet com o mesmo nome, ele será sobrescrito! (OVERWRITE)")
df_curated.write.mode("overwrite").parquet("data/curated/microsoft_repos")
print("Dados salvos com sucesso!")


print("Encerrando a sessão spark...")
spark.stop()
print("===============================================")

