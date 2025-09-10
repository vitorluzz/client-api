import os
import time
import logging
import pandas as pd
from datetime import datetime
from solution.helpers.github_helper import GitHub

logger = logging.getLogger(__name__)

class GitHubExtractor:
    def __init__(self, job_stage_id: str, job_stage_name: str):
        self.job_stage_id = job_stage_id
        self.job_stage_name = job_stage_name
        self.output_path = "data/raw"
        self._setup_directories()

    def _setup_directories(self):
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    def extract(self):
        logger.info(f"Iniciando o job stage: {self.job_stage_id} - {self.job_stage_name}")
        start_time = time.time()

        api_github = GitHub()
        organizations = ["microsoft", "aws", "oracle", "ibm", "apache"]
        logger.info("Iniciando coleta dos repositórios das organizações.")

        all_data = api_github.get_repos_orgs(organizations)

        if all_data:
            logger.info("Convertendo dados para DataFrame.")
            df_all = pd.DataFrame.from_dict(all_data)
            campos = [
                "id", "full_name", "name", "language", "created_at", "updated_at",
                "size", "stargazers_count", "watchers_count", "forks_count",
                "open_issues_count", "archived", "disabled", "visibility"
            ]
            df_all = df_all[campos]

            output_file = os.path.join(self.output_path, "report_1.parquet")
            df_all.to_parquet(output_file, index=False)
            logger.info(f"✅ Arquivo salvo em: {output_file}")
        else:
            logger.warning("⚠️ Nenhum dado encontrado para salvar.")

        end_time = time.time()
        logger.info(f"⏱️ Tempo total de execução: {end_time - start_time:.2f} segundos")
        print("✔️ Processo de extração finalizado com sucesso!")

def run_extraction():
    print("=" * 66)
    print('''

  ██████╗██╗     ██╗███████╗███╗   ██╗████████╗    █████╗ ██████╗ ██╗
 ██╔════╝██║     ██║██╔════╝████╗  ██║╚══██╔══╝   ██╔══██╗██╔══██╗██║
 ██║     ██║     ██║█████╗  ██╔██╗ ██║   ██║█████╗███████║██████╔╝██║
 ██║     ██║     ██║██╔══╝  ██║╚██╗██║   ██║╚════╝██╔══██║██╔═══╝ ██║
 ╚██████╗███████╗██║███████╗██║ ╚████║   ██║      ██║  ██║██║     ██║
  ╚═════╝╚══════╝╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝      ╚═╝  ╚═╝╚═╝     ╚═╝

''')
    print("Iniciando o processo de extração de dados do GitHub...")
    print("=" * 66)

    extractor = GitHubExtractor(
        job_stage_id="raw_01",
        job_stage_name="Extração de Repositórios do GitHub"
    )
    extractor.extract()
