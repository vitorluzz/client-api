import os
import logging
from datetime import datetime

from solution.job_stage_raw import GitHubExtractor
from solution.job_stage_curated import SparkCurator
from solution.job_stage_analytics import AnalyticsGenerator

# === Configuração de Logging Central ===
LOG_DATE = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, f"application_{LOG_DATE}.log")

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')  # 'w' para sobrescrever caso exista
    ]
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    print("=" * 66)
    print('''

  ██████╗██╗     ██╗███████╗███╗   ██╗████████╗    █████╗ ██████╗ ██╗
 ██╔════╝██║     ██║██╔════╝████╗  ██║╚══██╔══╝   ██╔══██╗██╔══██╗██║
 ██║     ██║     ██║█████╗  ██╔██╗ ██║   ██║█████╗███████║██████╔╝██║
 ██║     ██║     ██║██╔══╝  ██║╚██╗██║   ██║╚════╝██╔══██║██╔═══╝ ██║
 ╚██████╗███████╗██║███████╗██║ ╚████║   ██║      ██║  ██║██║     ██║
  ╚═════╝╚══════╝╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝      ╚═╝  ╚═╝╚═╝     ╚═╝

''')
    start_time = datetime.now()
    logger.info(f"Início do pipeline às {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Etapa 1 - Raw
    stage_raw = GitHubExtractor(
        job_stage_id="1",
        job_stage_name="Data Ingestion",
    )
    stage_raw.extract()

    # Etapa 2 - Curated
    stage_curated = SparkCurator(
        job_stage_id="2",
        job_stage_name="Data Cleaning"
    )
    stage_curated.start_spark()
    df_raw = stage_curated.read_raw_data()
    df_curated = stage_curated.transform_data(df_raw)
    stage_curated.save_data(df_curated)

    # Etapa 3 - Analytics
    spark_session = stage_curated.get_spark()
    stage_analytics = AnalyticsGenerator(
        job_stage_id="3",
        job_stage_name="Data Analysis",
        spark=spark_session
    )
    stage_analytics.run()

    stage_curated.stop_spark()

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Pipeline finalizado às {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duração total: {duration}")
