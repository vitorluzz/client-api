import requests
import os
import logging
import time

logger = logging.getLogger(__name__)

class GitHub:
    def __init__(self):
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.github_url = 'https://api.github.com'
        if not self.github_token:
            raise Exception("O token do GitHub não foi encontrado nas variáveis de ambiente!")

    def get_auth_header(self):
        logger.debug("Obtendo header de autenticação.")
        return {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.github_token}"
        }

    def get_repos(self, org, per_page=100, retries=5, timeout=10):
        logger.info(f"Buscando repositórios da organização: {org}")
        all_repos = []
        page = 1

        while True:
            url = f"{self.github_url}/orgs/{org}/repos"
            params = {"per_page": per_page, "page": page}
            logger.debug(f"URL: {url}, Página: {page}")

            for attempt in range(1, retries + 1):
                try:
                    logger.debug(f"[{org}] Página {page}, Tentativa {attempt}")
                    response = requests.get(
                        url,
                        headers=self.get_auth_header(),
                        params=params,
                        timeout=timeout
                    )
                    response.raise_for_status()

                    data = response.json()
                    if not data:
                        logger.info(f"Finalizada busca de repositórios para {org}. Total: {len(all_repos)}")
                        return all_repos

                    for repo in data:
                        logger.info(
                            f"📦 [{org}] Repo: {repo.get('full_name')} | Lang: {repo.get('language')} | ⭐: {repo.get('stargazers_count')}"
                        )

                    all_repos.extend(data)
                    if len(data) < per_page:
                        logger.info(f"Todos os repositórios recuperados para {org}.")
                        return all_repos

                    page += 1
                    break

                except requests.exceptions.RequestException as e:
                    logger.error(f"❌ [{org}] Tentativa {attempt} falhou: {e}")
                    if attempt < retries:
                        wait = 2 ** attempt
                        logger.info(f"⏳ Aguardando {wait}s antes da próxima tentativa...")
                        time.sleep(wait)
                    else:
                        logger.critical(f"🚫 Todas as tentativas falharam para {org}, página {page}. Abortando.")
                        return all_repos

    def get_repos_orgs(self, orgs, per_page=100, retries=5, timeout=10):
        logger.info("Iniciando coleta de repositórios para múltiplas organizações.")
        all_data = []
        for org in orgs:
            repos = self.get_repos(org, per_page, retries, timeout)
            if repos:
                all_data.extend(repos)
        return all_data
