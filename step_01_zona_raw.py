import requests
import os
import pandas as pd
import logging
import time
from datetime import datetime

# ========================== CONFIGURAÇÃO DO LOGGING ==========================

print("================================================================")
print('''
  
  ██████╗██╗     ██╗███████╗███╗   ██╗████████╗    █████╗ ██████╗ ██╗
 ██╔════╝██║     ██║██╔════╝████╗  ██║╚══██╔══╝   ██╔══██╗██╔══██╗██║
 ██║     ██║     ██║█████╗  ██╔██╗ ██║   ██║█████╗███████║██████╔╝██║
 ██║     ██║     ██║██╔══╝  ██║╚██╗██║   ██║╚════╝██╔══██║██╔═══╝ ██║
 ╚██████╗███████╗██║███████╗██║ ╚████║   ██║      ██║  ██║██║     ██║
  ╚═════╝╚══════╝╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝      ╚═╝  ╚═╝╚═╝     ╚═╝
                                                        
''')
print
print("Iniciando o processo de extração de dados do github...")

log_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = f"logs/github_repos_{log_timestamp}.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    ]
)

# ========================== PARÂMETROS GERAIS ==========================
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_URL = "https://api.github.com"
organizations = ["microsoft", "aws", "oracle", "ibm", "apache"]

# ========================== FUNÇÕES ==========================

def get_auth_headers():
    return {"Authorization": f"token {GITHUB_TOKEN}"}

def get_repos(org, per_page=100):
    all_repos = []
    page = 1

    headers = {
        "Accept": "application/vnd.github.v3+json"
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        params = {"per_page": per_page, "page": page}
        response = requests.get(url, params=params, headers=headers)

        if response.status_code != 200:
            logging.error(f"Erro ao buscar dados da organização {org} (página {page}): {response.status_code}")
            break

        data = response.json()

        if not data:
            break

        for repo in data:
            logging.info(
                f"📦 [{org}] Repo: {repo.get('full_name')} | Lang: {repo.get('language')} | ⭐: {repo.get('stargazers_count')}"
            )

        all_repos.extend(data)
        page += 1

    return all_repos

# ========================== EXECUÇÃO PRINCIPAL ==========================

all_dfs = []
start_time = time.time()

for org in organizations:
    logging.info(f"🔍 Buscando repositórios da organização: {org}")
    repos = get_repos(org)
    
    if not repos:
        logging.warning(f"Nenhum repositório encontrado para {org}.")
        continue

    df = pd.DataFrame.from_dict(repos)

    campos = [
        "id", "full_name", "name", "language", "created_at", "updated_at",
        "size", "stargazers_count", "watchers_count", "forks_count",
        "open_issues_count", "archived", "disabled", "visibility"
    ]

    df = df[campos]
    all_dfs.append(df)

# ========================== SALVAMENTO ==========================

if all_dfs:
    df_all = pd.concat(all_dfs, ignore_index=True)
    
    os.makedirs("data/raw", exist_ok=True)
    df_all.to_parquet("data/raw/report_1.parquet", index=False)

    logging.info("✅ Arquivo salvo em: data/raw/report_1.parquet")
else:
    logging.warning("⚠️ Nenhum dado encontrado para salvar.")

# ========================== TEMPO TOTAL ==========================

print("✔️ Processo de extração finalizado com sucesso!")

end_time = time.time()
elapsed_time = end_time - start_time
logging.info(f"⏱️ Tempo total de execução: {elapsed_time:.2f} segundos")

print("===============================================================")