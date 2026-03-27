# ANVISA VigiMed - Dashboard de Farmacovigilancia

> Analise exploratoria de eventos adversos a medicamentos no Brasil, com dados do sistema VigiMed da ANVISA.

**Projeto Final** | Ciencia de Dados | Engenharia de Software - PUC Campinas | 2026

---

## Sobre o Projeto

Este projeto analisa o panorama da farmacovigilancia no Brasil cruzando **4 datasets publicos da ANVISA**, totalizando mais de **1,8 milhao de registros**:

| Dataset | Registros | Descricao |
|---------|-----------|-----------|
| Medicamentos Registrados | 32.338 | Base completa de medicamentos com registro ativo/inativo |
| VigiMed - Notificacoes | 311.134 | Notificacoes de eventos adversos (dados demograficos, gravidade, desfecho) |
| VigiMed - Medicamentos | 589.421 | Medicamentos envolvidos nas notificacoes (suspeitos e concomitantes) |
| VigiMed - Reacoes | 873.682 | Reacoes adversas classificadas pela hierarquia MedDRA |

O dashboard permite identificar **padroes, tendencias e insights** como:
- Quais classes terapeuticas concentram mais eventos graves
- Evolucao temporal das notificacoes (incluindo picos em periodos de vacinacao)
- Disparidades geograficas na notificacao entre estados
- Perfil demografico dos pacientes afetados
- Relacao entre sistemas organicos afetados e tipos de medicamento

---

## Pipeline de Dados

```
[1] COLETA           [2] LIMPEZA           [3] INTEGRACAO         [4] TRANSFORMACAO

 API OAuth2 ANVISA    Encoding latin-1      Star schema VigiMed    Variaveis derivadas
 Download 4 CSVs      4 formatos de data    Fuzzy match por        Faixa etaria, regiao
 ~393 MB total         Parsing de idade      principio ativo       ATC levels, gravidade
 Barra de progresso   Dedup, padronizacao   38.5% match rate       8 tabelas agregadas
```

### Coleta Automatica (API + CSV)

O projeto implementa **coleta automatica de dados** de duas formas:

1. **API OAuth2 da ANVISA** - Autenticacao via Keycloak no endpoint `acesso.prd.apps.anvisa.gov.br`, obtendo token para consultas no API Gateway
2. **Download automatizado** - Script que baixa os 4 CSVs do portal `dados.anvisa.gov.br` com streaming e barra de progresso

### Integracao

Os datasets sao integrados em um **star schema**:

```
Notificacoes (fato)
    |-- Medicamentos Suspeitos (1:N por IDENTIFICACAO_NOTIFICACAO)
    |-- Reacoes Adversas (1:N por IDENTIFICACAO_NOTIFICACAO)
    |-- Medicamentos Registrados (N:1 por principio ativo normalizado)
```

---

## Dashboards

### Dashboard 1 - Visao Executiva

Painel resumido com os principais indicadores:
- **4 KPI Cards** - Medicamentos registrados, total de notificacoes, taxa de gravidade, medicamento mais reportado
- **Tendencia temporal** - Evolucao mensal com linha de eventos graves
- **Top classes terapeuticas** - Ranking por volume de notificacoes
- **Gravidade** - Proporcao grave vs. nao grave
- **Desfechos** - Top 10 medicamentos por tipo de desfecho
- **Insights automaticos** - Bullets gerados a partir dos dados

### Dashboard 2 - Exploracao Interativa

Painel com filtros dinamicos e 7 visualizacoes:

**Filtros:** Periodo (ano), Classe terapeutica, Regiao/UF, Gravidade, Categoria regulatoria, Top N

**Graficos:**
1. Top N medicamentos por notificacoes (grave vs. nao grave)
2. Tendencia temporal com breakdown (gravidade / regiao)
3. Scatter: volume de notificacoes vs. taxa de gravidade
4. Heatmap: classe terapeutica vs. sistema organico (SOC)
5. Treemap: hierarquia MedDRA (SOC > HLGT)
6. Distribuicao geografica por UF
7. Distribuicao de desfechos (donut)

---

## Como Executar

### Pre-requisitos

- Python 3.11+
- ~500 MB de espaco em disco (para os dados brutos)

### Instalacao

```bash
git clone https://github.com/GabrieelCostaa/anvisa-dashboard.git
cd anvisa-dashboard

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuracao (opcional - para API OAuth2)

Crie um arquivo `.env` na raiz do projeto:

```env
ANVISA_CLIENT_ID=seu_client_id
ANVISA_CLIENT_SECRET=seu_client_secret
```

As credenciais podem ser obtidas no portal [api.anvisa.gov.br](https://api.anvisa.gov.br). O projeto funciona sem elas (usa os CSVs publicos).

### Executar o Pipeline

```bash
python run_pipeline.py
```

Isso ira:
1. Tentar coleta via API ANVISA (ponto bonus)
2. Baixar os 4 datasets CSV (~393 MB)
3. Limpar e tratar os dados
4. Integrar os datasets (star schema + fuzzy match)
5. Gerar variaveis derivadas e tabelas de agregacao

> Se os dados ja foram baixados: `python run_pipeline.py --skip-download`

### Iniciar o Dashboard

```bash
python -m src.dashboard.app
```

Acesse em: **http://localhost:8050**

---

## Estrutura do Projeto

```
anvisa-dashboard/
├── src/
│   ├── collector/              # Coleta automatica de dados
│   │   ├── anvisa_api.py       # Cliente OAuth2 (Keycloak)
│   │   └── download.py         # Download CSVs + tentativa API
│   ├── pipeline/               # Pipeline de dados
│   │   ├── clean.py            # Limpeza e tratamento
│   │   ├── integrate.py        # Integracao e merge
│   │   └── transform.py        # Variaveis derivadas e agregacoes
│   ├── analysis/               # Analise exploratoria
│   │   └── eda.py              # Estatisticas e geracao de insights
│   └── dashboard/              # Aplicacao Dash
│       ├── app.py              # Entry point
│       ├── layout.py           # Layout com tabs
│       ├── overview.py         # Dashboard 1: Visao Executiva
│       ├── explorer.py         # Dashboard 2: Exploracao Interativa
│       ├── components.py       # Componentes reutilizaveis
│       └── styles.py           # Paleta de cores e estilos
├── data/
│   ├── raw/                    # Dados brutos (gitignored)
│   └── processed/              # Parquets processados (gitignored)
├── notebooks/                  # Notebooks de EDA
├── run_pipeline.py             # Orquestrador do pipeline
├── requirements.txt
└── .env                        # Credenciais (gitignored)
```

## Tecnologias

| Camada | Stack |
|--------|-------|
| Dashboard | Dash 2.18, Dash Bootstrap Components, Plotly |
| Dados | Pandas, PyArrow (Parquet) |
| Coleta | Requests, OAuth2/Keycloak |
| Matching | FuzzyWuzzy + Python-Levenshtein |
| Design | Principios de Storytelling with Data (Cole Nussbaumer Knaflic) |

## Fontes de Dados

Todos os dados sao publicos e disponibilizados pela ANVISA:

- [Medicamentos Registrados](https://dados.anvisa.gov.br/dados/DADOS_ABERTOS_MEDICAMENTOS.csv)
- [VigiMed - Notificacoes](https://dados.anvisa.gov.br/dados/VigiMed_Notificacoes.csv)
- [VigiMed - Medicamentos](https://dados.anvisa.gov.br/dados/VigiMed_Medicamentos.csv)
- [VigiMed - Reacoes](https://dados.anvisa.gov.br/dados/VigiMed_Reacoes.csv)

## Autores

- **Gabriel Costa** - [GabrieelCostaa](https://github.com/GabrieelCostaa)
- **Henrique Monteiro** - [HenriqueVMonteiro](https://github.com/HenriqueVMonteiro)

---

*Projeto desenvolvido como trabalho final da disciplina de Ciencia de Dados — Engenharia de Software, PUC Campinas.*
