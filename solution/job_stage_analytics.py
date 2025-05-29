import os
import pandas as pd
import sqlite3
import time
import logging

class AnalyticsGenerator:
    def __init__(self, job_stage_id, job_stage_name, spark):
        self.job_stage_id = job_stage_id
        self.job_stage_name = job_stage_name
        self.spark = spark
        self.input_path = "data/curated/report_2.parquet"
        self.output_path = "data/analytics/report_3.parquet"
        self.db_path = "github.db"

    def read_data(self):
        logging.info(f"📥 [{self.job_stage_id}] Lendo arquivo curated do caminho: {self.input_path}")
        df_curated = self.spark.read.parquet(self.input_path)
        df_curated.show()
        df_curated.printSchema()
        return df_curated

    def save_to_parquet(self, df):
        logging.info(f"💾 [{self.job_stage_id}] Salvando dados em {self.output_path} (append mode)")
        df.write.mode("append").parquet(self.output_path)

    def save_to_sqlite(self):
        logging.info(f"🛢️ [{self.job_stage_id}] Salvando dados no banco SQLite {self.db_path}")
        df = pd.read_parquet(self.output_path)
        conn = sqlite3.connect(self.db_path)
        df.to_sql("repos", conn, if_exists="replace", index=False)
        conn.commit()
        conn.close()
        logging.info(f"✅ [{self.job_stage_id}] Dados gravados com sucesso no SQLite.")

    def run(self):
        start_time = time.time()
        logging.info(f"🚦 [{self.job_stage_id}] Iniciando etapa {self.job_stage_name}...")

        try:
            df = self.read_data()
            total_rows = df.count()
            logging.info(f"📊 [{self.job_stage_id}] Total de registros lidos: {total_rows:,}")
            self.save_to_parquet(df)
        except Exception as e:
            logging.error(f"❌ [{self.job_stage_id}] Erro na etapa {self.job_stage_name}: {e}", exc_info=True)
        finally:
            self.save_to_sqlite()
            elapsed = time.time() - start_time
            logging.info(f"⏱️ [{self.job_stage_id}] Etapa {self.job_stage_name} finalizada em {elapsed:.2f}s")
