import os
import time
import shutil
import glob
import logging

from pyspark.sql import SparkSession, functions as F, types as T, Window


class SparkCurator:
    def __init__(self, job_stage_id, job_stage_name):
        self.job_stage_id = job_stage_id
        self.job_stage_name = job_stage_name
        self.output_path = "data/curated"
        self.temp_path = os.path.join(self.output_path, "_temp")
        self.final_file_path = os.path.join(self.output_path, "report_2.parquet")
        self.spark = None

    def start_spark(self):
        logging.info(f"🚀 [{self.job_stage_id} - {self.job_stage_name}] Criando a sessão Spark...")
        self.spark = SparkSession.builder \
            .appName("Zona Curated") \
            .master("local[*]") \
            .getOrCreate()

    def get_spark(self):
        return self.spark

    def read_raw_data(self, path="data/raw/report_1.parquet"):
        logging.info(f"📥 [{self.job_stage_id}] Lendo dados brutos do caminho: {path}")
        df_raw = self.spark.read.format("parquet").load(path)
        df_raw.printSchema()
        df_raw.show()
        return df_raw

    def transform_data(self, df_raw):
        logging.info(f"🧹 [{self.job_stage_id}] Transformando dados...")
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
        window_spec = Window.partitionBy("language")
        df_curated = df_curated.withColumn("language_count", F.count("language").over(window_spec))
        df_curated = df_curated.withColumn("processed_at", F.current_date())

        logging.info(f"📊 [{self.job_stage_id}] Dados transformados:")
        df_curated.show(truncate=False)
        return df_curated

    def save_data(self, df_curated):
        logging.info(f"💾 [{self.job_stage_id}] Salvando dados no caminho: {self.final_file_path}")
        os.makedirs(self.output_path, exist_ok=True)

        if os.path.exists(self.final_file_path):
            logging.info(f"📂 [{self.job_stage_id}] Dados antigos encontrados. Fazendo merge...")
            df_old = self.spark.read.parquet(self.final_file_path)
            df_curated = df_old.unionByName(df_curated)

        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

        df_curated.coalesce(1).write.mode("overwrite").parquet(self.temp_path)

        part_file = glob.glob(os.path.join(self.temp_path, "part-*.parquet"))[0]
        if os.path.exists(self.final_file_path):
            os.remove(self.final_file_path)
        shutil.move(part_file, self.final_file_path)
        shutil.rmtree(self.temp_path)

        logging.info(f"✅ [{self.job_stage_id}] Dados salvos em {self.final_file_path}")

    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            logging.info(f"🛑 [{self.job_stage_id}] Sessão Spark finalizada.")
