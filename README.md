# Simple Client API

```

             ██████╗██╗     ██╗███████╗███╗   ██╗████████╗    █████╗ ██████╗ ██╗
            ██╔════╝██║     ██║██╔════╝████╗  ██║╚══██╔══╝   ██╔══██╗██╔══██╗██║
            ██║     ██║     ██║█████╗  ██╔██╗ ██║   ██║█████╗███████║██████╔╝██║
            ██║     ██║     ██║██╔══╝  ██║╚██╗██║   ██║╚════╝██╔══██║██╔═══╝ ██║
            ╚██████╗███████╗██║███████╗██║ ╚████║   ██║      ██║  ██║██║     ██║
             ╚═════╝╚══════╝╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝      ╚═╝  ╚═╝╚═╝     ╚═╝


```

Este projeto em Python, baseado em engenharia de dados, realiza um processo completo de ETL (Extração, Transformação e Carga) utilizando dados públicos da API do GitHub sobre repositórios de empresas de tecnologia. Ele foi desenvolvido com foco em coleta, padronização e consolidação de dados para análises futuras.

## ✨ Funcionalidades

- 🔍 Consulta automática à API do GitHub buscando repositórios de empresas de tecnologia.
- 🛠️ Transformação dos dados com tratamento e padronização.
- 📦 Armazenamento em formato Parquet com controle de versão e consolidação.
- 🧪 Preparação da zona curated para consumo posterior (por dashboards, notebooks etc.).
- 🧠 Utilização de PySpark para processamento em larga escala.
- 📊 **Data Observability:** monitoração contínua da qualidade, frescor, completude e consistência dos dados ao longo do pipeline, permitindo detecção proativa de anomalias e garantindo confiança nos dados utilizados para análise e tomada de decisão.

## ⚙️ Requisitos

Para executar o projeto, certifique-se de ter instalado:

- Configurar um GitHub Token em suas variáveis de ambiente
- Python 3.8 ou superior
- Java (necessário para Spark)
- Apache Spark (compatível com o PySpark instalado)

---

## 🚀 Passo a passo - Como executar

**1** - Clone o repositório:

```bash
git clone https://github.com/vitorluzz/client-api.git
cd client-api
```

---

**2** - Criação do ambiente virtual python:

```bash
python -m venv .venv && . .venv/bin/activate && python -m pip install --upgrade pip
```

Ou

```bash
python3 -m venv .venv && . .venv/bin/activate && python -m pip install --upgrade pip
```

---

**3** - Instale as dependências:

```bash
pip install -r requirements.txt
```

---

**4** - Configurar as variáveis de ambiente:

**4.1** Acesse o bashrc:
```bash
nano ~/.bashrc
```

**4.2** Adicione o token GitHub ao final do arquivo bashrc:
```bash
# GITHUB
export GITHUB_TOKEN="seu_token_aqui"
```

**4.3** Salve o arquivo: 
```
CTRL O + ENTER + CTRL X
```

**4.4** Então, atualize o bashrc para refletir as alterações:
```bash
source ~/.bashrc
```

---

**5** - Execute o script main:

```bash
python main.py
```

