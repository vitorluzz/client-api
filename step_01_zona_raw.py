import requests
import os
import pandas as pd

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_URL = "https://api.github.com"

def get_auth_headers():
    return {"Authorization": f"token {GITHUB_TOKEN}"}

def get_user(): 
    resposta = requests.get(f"{GITHUB_URL}/user", headers = get_auth_headers())
    return resposta.json()

def get_repos():
    resposta = requests.get(f"{GITHUB_URL}/user/repos", headers = get_auth_headers())
    return resposta.json()

def get_repos(organization):
    resposta = requests.get(f"{GITHUB_URL}/orgs/{organization}/repos", headers = get_auth_headers())
    return resposta.json()

# Montando a zona raw
data_raw = get_repos("microsoft")

df_microsoft = pd.DataFrame.from_dict(data_raw)
df_microsoft = df_microsoft[["id", "full_name", "name", "language", "created_at", "updated_at"]]
print(df_microsoft)
df_microsoft.to_csv("data/raw/microsoft_repos.csv", index=False)


