# Dashboard ANVISA - Farmacovigilancia no Brasil

Dashboard interativo para analise de dados de farmacovigilancia da ANVISA, cruzando medicamentos registrados com eventos adversos do sistema VigiMed.

## Requisitos

- Python 3.11+

## Instalacao

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

### 1. Executar o pipeline de dados

```bash
python run_pipeline.py
```

Isso ira:
- Baixar os 4 datasets da ANVISA (medicamentos + VigiMed)
- Limpar e tratar os dados
- Integrar os datasets
- Gerar tabelas de agregacao

### 2. Iniciar o dashboard

```bash
python -m src.dashboard.app
```

Acesse em: http://localhost:8050

## Estrutura

- `src/collector/` - Coleta automatica via API ANVISA + download de CSVs
- `src/pipeline/` - Limpeza, integracao e transformacao dos dados
- `src/analysis/` - Funcoes de analise exploratoria
- `src/dashboard/` - Aplicacao Dash com 2 dashboards
- `data/raw/` - Dados brutos (gitignored)
- `data/processed/` - Dados processados em Parquet (gitignored)

## Fontes de Dados

- [Medicamentos Registrados](https://dados.anvisa.gov.br/dados/DADOS_ABERTOS_MEDICAMENTOS.csv)
- [VigiMed Notificacoes](https://dados.anvisa.gov.br/dados/VigiMed_Notificacoes.csv)
- [VigiMed Medicamentos](https://dados.anvisa.gov.br/dados/VigiMed_Medicamentos.csv)
- [VigiMed Reacoes](https://dados.anvisa.gov.br/dados/VigiMed_Reacoes.csv)
