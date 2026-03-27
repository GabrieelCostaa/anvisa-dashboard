"""
Transformação de dados: variáveis derivadas e tabelas de agregação.
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

UF_TO_REGIAO = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte",
    "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}

ATC_NIVEL1_NOMES = {
    "A": "Aparelho digestivo",
    "B": "Sangue e orgaos hematopoieticos",
    "C": "Aparelho cardiovascular",
    "D": "Dermatologicos",
    "G": "Aparelho geniturinario",
    "H": "Hormonios sistemicos",
    "J": "Anti-infecciosos",
    "L": "Antineoplasicos",
    "M": "Sistema musculoesqueletico",
    "N": "Sistema nervoso",
    "P": "Antiparasitarios",
    "R": "Aparelho respiratorio",
    "S": "Orgaos dos sentidos",
    "V": "Varios",
}


def add_derived_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona variáveis derivadas ao dataset integrado."""
    logger.info("=== Adicionando variaveis derivadas ===")

    # Região a partir da UF
    if "UF" in df.columns:
        df["REGIAO"] = df["UF"].map(UF_TO_REGIAO)

    # Faixa etária
    if "IDADE_ANOS" in df.columns:
        bins = [0, 17, 39, 59, 79, 150]
        labels = ["0-17", "18-39", "40-59", "60-79", "80+"]
        df["FAIXA_ETARIA"] = pd.cut(df["IDADE_ANOS"], bins=bins, labels=labels, right=True)

    # Ano e mês da notificação
    date_col = None
    for candidate in ["DATA_INCLUSAO_SISTEMA", "DATA_NOTIFICACAO"]:
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col:
        df["ANO_NOTIFICACAO"] = df[date_col].dt.year
        df["MES_ANO"] = df[date_col].dt.to_period("M").astype(str)

    # Nome do ATC nível 1
    if "ATC_NIVEL1" in df.columns:
        df["ATC_NIVEL1_NOME"] = df["ATC_NIVEL1"].map(ATC_NIVEL1_NOMES)

    # Sexo padronizado
    if "SEXO" in df.columns:
        df["SEXO"] = df["SEXO"].astype(str).str.strip().str.upper()
        df["SEXO"] = df["SEXO"].replace({"MASCULINO": "M", "FEMININO": "F", "NAN": np.nan})

    logger.info("  Variaveis derivadas adicionadas")
    return df


def build_aggregations(df: pd.DataFrame):
    """Constrói tabelas de agregação pré-computadas para o dashboard."""
    logger.info("=== Construindo tabelas de agregacao ===")

    # 1. Tendência mensal
    if "MES_ANO" in df.columns:
        agg_monthly = df.groupby("MES_ANO").agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
        ).reset_index()
        agg_monthly["NAO_GRAVES"] = agg_monthly["TOTAL"] - agg_monthly["GRAVES"]
        agg_monthly = agg_monthly.sort_values("MES_ANO")
        agg_monthly.to_parquet(PROCESSED_DIR / "agg_monthly_trend.parquet", index=False)
        logger.info("  agg_monthly_trend: %d periodos", len(agg_monthly))

    # 2. Por classe terapêutica
    if "ATC_NIVEL1_NOME" in df.columns:
        agg_class = df.groupby(["ATC_NIVEL1", "ATC_NIVEL1_NOME"]).agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
            MEDIA_IDADE=("IDADE_ANOS", "mean"),
        ).reset_index()
        agg_class["TAXA_GRAVIDADE"] = (agg_class["GRAVES"] / agg_class["TOTAL"]).round(3)
        agg_class = agg_class.sort_values("TOTAL", ascending=False)
        agg_class.to_parquet(PROCESSED_DIR / "agg_by_therapeutic_class.parquet", index=False)
        logger.info("  agg_by_therapeutic_class: %d classes", len(agg_class))

    # 3. Por UF
    if "UF" in df.columns and "REGIAO" in df.columns:
        agg_uf = df.groupby(["UF", "REGIAO"]).agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
        ).reset_index()
        agg_uf["TAXA_GRAVIDADE"] = (agg_uf["GRAVES"] / agg_uf["TOTAL"]).round(3)
        agg_uf = agg_uf.sort_values("TOTAL", ascending=False)
        agg_uf.to_parquet(PROCESSED_DIR / "agg_by_uf.parquet", index=False)
        logger.info("  agg_by_uf: %d UFs", len(agg_uf))

    # 4. Por SOC (System Organ Class) - a partir das reações
    reac_path = PROCESSED_DIR / "vigimed_reacoes.parquet"
    if reac_path.exists():
        reac = pd.read_parquet(reac_path)
        if "SOC" in reac.columns:
            agg_soc = reac.groupby("SOC").agg(
                TOTAL=("IDENTIFICACAO_NOTIFICACAO", "count"),
                GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
            ).reset_index()
            agg_soc["TAXA_GRAVIDADE"] = (agg_soc["GRAVES"] / agg_soc["TOTAL"]).round(3)
            agg_soc = agg_soc.sort_values("TOTAL", ascending=False)
            agg_soc.to_parquet(PROCESSED_DIR / "agg_by_soc.parquet", index=False)
            logger.info("  agg_by_soc: %d SOCs", len(agg_soc))

    # 5. Por empresa
    if "NOME_EMPRESA" in df.columns:
        col = "NOME_EMPRESA"
    elif "DETENTOR_REGISTRO" in df.columns:
        col = "DETENTOR_REGISTRO"
    else:
        col = None

    if col:
        agg_emp = df.groupby(col).agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
        ).reset_index()
        agg_emp.columns = ["EMPRESA", "TOTAL", "GRAVES"]
        agg_emp["TAXA_GRAVIDADE"] = (agg_emp["GRAVES"] / agg_emp["TOTAL"]).round(3)
        agg_emp = agg_emp.sort_values("TOTAL", ascending=False)
        agg_emp.to_parquet(PROCESSED_DIR / "agg_by_company.parquet", index=False)
        logger.info("  agg_by_company: %d empresas", len(agg_emp))

    # 6. Por desfecho
    if "DESFECHO" in df.columns:
        agg_desf = df.groupby("DESFECHO").agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
        ).reset_index()
        agg_desf = agg_desf.sort_values("TOTAL", ascending=False)
        agg_desf.to_parquet(PROCESSED_DIR / "agg_by_outcome.parquet", index=False)
        logger.info("  agg_by_outcome: %d desfechos", len(agg_desf))

    # 7. Por faixa etária e sexo
    cols_demo = [c for c in ["FAIXA_ETARIA", "SEXO"] if c in df.columns]
    if cols_demo:
        agg_demo = df.groupby(cols_demo).agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
        ).reset_index()
        agg_demo.to_parquet(PROCESSED_DIR / "agg_by_demographics.parquet", index=False)
        logger.info("  agg_by_demographics: %d grupos", len(agg_demo))

    # 8. Por categoria regulatória
    if "CATEGORIA_REGULATORIA_REG" in df.columns:
        agg_cat = df.groupby("CATEGORIA_REGULATORIA_REG").agg(
            TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
        ).reset_index()
        agg_cat["TAXA_GRAVIDADE"] = (agg_cat["GRAVES"] / agg_cat["TOTAL"]).round(3)
        agg_cat = agg_cat.sort_values("TOTAL", ascending=False)
        agg_cat.to_parquet(PROCESSED_DIR / "agg_by_regulatory.parquet", index=False)
        logger.info("  agg_by_regulatory: %d categorias", len(agg_cat))


def transform_all():
    """Executa todas as transformações."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Carregar integrado
    integrated_path = PROCESSED_DIR / "integrated_notifications.parquet"
    if not integrated_path.exists():
        raise FileNotFoundError(f"{integrated_path} nao encontrado. Execute integrate.py primeiro.")

    df = pd.read_parquet(integrated_path)
    print(f"  Carregado: {len(df):,} linhas")

    # Adicionar variáveis derivadas
    df = add_derived_variables(df)

    # Salvar integrado atualizado
    df.to_parquet(integrated_path, index=False)
    print(f"  Integrado atualizado com variaveis derivadas")

    # Construir agregações
    build_aggregations(df)
    print("  Tabelas de agregacao criadas")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform_all()
