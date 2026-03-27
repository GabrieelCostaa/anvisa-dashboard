"""
Limpeza e tratamento dos dados brutos da ANVISA.
Trata encoding, datas, nulos, duplicatas e salva como Parquet.
"""

import re
import logging
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def _read_anvisa_csv(filename: str, **kwargs) -> pd.DataFrame:
    """Lê CSV da ANVISA com encoding e separador corretos."""
    path = RAW_DIR / filename
    logger.info("Lendo %s...", path)
    df = pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False, on_bad_lines="skip", **kwargs)
    logger.info("  %d linhas x %d colunas", len(df), len(df.columns))
    return df


def _parse_age_to_years(age_series: pd.Series) -> pd.Series:
    """Converte '30 ano', '6 mes', '5 dia' para anos (float)."""
    def _parse(val):
        if pd.isna(val) or str(val).strip() == "":
            return np.nan
        val = str(val).lower().strip()
        match = re.match(r"(\d+\.?\d*)\s*(ano|mes|dia|hora|semana)", val)
        if not match:
            try:
                return float(val)
            except ValueError:
                return np.nan
        num = float(match.group(1))
        unit = match.group(2)
        if unit == "ano":
            return num
        elif unit == "mes":
            return num / 12
        elif unit == "semana":
            return num / 52
        elif unit == "dia":
            return num / 365
        elif unit == "hora":
            return num / 8760
        return np.nan

    return age_series.apply(_parse)


def _split_empresa(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Separa 'CNPJ - Nome Empresa' em duas colunas."""
    parts = series.str.split(r"\s*-\s*", n=1, expand=True)
    cnpj = parts[0].str.strip() if 0 in parts.columns else pd.Series(dtype=str)
    nome = parts[1].str.strip() if 1 in parts.columns else series
    return cnpj, nome


def clean_medicamentos() -> pd.DataFrame:
    """Limpa dataset de medicamentos registrados."""
    logger.info("=== Limpando Medicamentos Registrados ===")
    df = _read_anvisa_csv("medicamentos.csv")

    # Padronizar nomes de colunas
    df.columns = df.columns.str.strip().str.upper()

    # Parsear datas
    if "DATA_FINALIZACAO_PROCESSO" in df.columns:
        df["DATA_FINALIZACAO_PROCESSO"] = pd.to_datetime(
            df["DATA_FINALIZACAO_PROCESSO"], format="%d/%m/%Y", errors="coerce"
        )

    if "DATA_VENCIMENTO_REGISTRO" in df.columns:
        def _parse_vencimento(val):
            if pd.isna(val) or str(val).strip() == "":
                return pd.NaT
            val = str(val).strip()
            if len(val) == 6:
                try:
                    return pd.Timestamp(year=int(val[2:]), month=int(val[:2]), day=1)
                except ValueError:
                    return pd.NaT
            return pd.to_datetime(val, errors="coerce")

        df["DATA_VENCIMENTO_REGISTRO"] = df["DATA_VENCIMENTO_REGISTRO"].apply(_parse_vencimento)

    # Separar empresa
    if "EMPRESA_DETENTORA_REGISTRO" in df.columns:
        df["CNPJ_EMPRESA"], df["NOME_EMPRESA"] = _split_empresa(df["EMPRESA_DETENTORA_REGISTRO"])

    # Padronizar texto
    for col in ["NOME_PRODUTO", "PRINCIPIO_ATIVO", "CLASSE_TERAPEUTICA", "CATEGORIA_REGULATORIA"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col] = df[col].replace("NAN", np.nan)

    # Remover linhas sem registro
    if "NUMERO_REGISTRO_PRODUTO" in df.columns:
        initial = len(df)
        df = df.dropna(subset=["NUMERO_REGISTRO_PRODUTO"])
        logger.info("  Removidas %d linhas sem numero de registro", initial - len(df))

    # Salvar
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / "medicamentos.parquet"
    df.to_parquet(out, index=False)
    logger.info("  Salvo %s (%d linhas)", out, len(df))
    return df


def clean_notificacoes() -> pd.DataFrame:
    """Limpa dataset de notificações VigiMed."""
    logger.info("=== Limpando VigiMed Notificacoes ===")
    df = _read_anvisa_csv("vigimed_notificacoes.csv")
    df.columns = df.columns.str.strip().str.upper()

    # Limpar "None" como string
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace({"None": np.nan, "none": np.nan, "NONE": np.nan})

    # Parsear datas
    for col_name, fmt in [
        ("DATA_NOTIFICACAO", None),
        ("DATA_INCLUSAO_SISTEMA", "%Y%m%d"),
    ]:
        if col_name in df.columns:
            if fmt:
                df[col_name] = pd.to_datetime(df[col_name].astype(str).str.strip(), format=fmt, errors="coerce")
            else:
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce", dayfirst=True)

    # Normalizar idade
    if "IDADE_MOMENTO_REACAO" in df.columns:
        df["IDADE_ANOS"] = _parse_age_to_years(df["IDADE_MOMENTO_REACAO"])

    # Converter grave para boolean
    if "GRAVE" in df.columns:
        df["GRAVE_BOOL"] = df["GRAVE"].astype(str).str.strip().str.upper().map({"SIM": True, "NAO": False, "NÃO": False})

    # Padronizar UF
    if "UF" in df.columns:
        df["UF"] = df["UF"].astype(str).str.strip().str.upper()
        df.loc[df["UF"].isin(["NAN", ""]), "UF"] = np.nan

    # Dedup
    if "IDENTIFICACAO_NOTIFICACAO" in df.columns:
        initial = len(df)
        df = df.drop_duplicates(subset=["IDENTIFICACAO_NOTIFICACAO"])
        logger.info("  Removidas %d duplicatas", initial - len(df))

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / "vigimed_notificacoes.parquet"
    df.to_parquet(out, index=False)
    logger.info("  Salvo %s (%d linhas)", out, len(df))
    return df


def clean_vigimed_medicamentos() -> pd.DataFrame:
    """Limpa dataset de medicamentos do VigiMed."""
    logger.info("=== Limpando VigiMed Medicamentos ===")
    df = _read_anvisa_csv("vigimed_medicamentos.csv")
    df.columns = df.columns.str.strip().str.upper()

    # Padronizar texto
    for col in ["NOME_MEDICAMENTO_WHODRUG", "PRINCIPIOS_ATIVOS_WHODRUG", "CODIGO_ATC",
                 "RELACAO_MEDICAMENTO_EVENTO", "VIA_ADMINISTRACAO", "DETENTOR_REGISTRO"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col] = df[col].replace("NAN", np.nan)

    # Extrair dose numérica
    if "DOSE" in df.columns:
        df["DOSE_VALOR"] = df["DOSE"].astype(str).str.extract(r"(\d+\.?\d*)")[0].astype(float)

    # ATC levels
    if "CODIGO_ATC" in df.columns:
        df["ATC_NIVEL1"] = df["CODIGO_ATC"].str[:1]
        df["ATC_NIVEL2"] = df["CODIGO_ATC"].str[:3]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / "vigimed_medicamentos.parquet"
    df.to_parquet(out, index=False)
    logger.info("  Salvo %s (%d linhas)", out, len(df))
    return df


def clean_vigimed_reacoes() -> pd.DataFrame:
    """Limpa dataset de reações adversas do VigiMed."""
    logger.info("=== Limpando VigiMed Reacoes ===")
    df = _read_anvisa_csv("vigimed_reacoes.csv")
    df.columns = df.columns.str.strip().str.upper()

    # Padronizar texto
    for col in ["REACAO_EVTO_ADVERSO_MEDDRA_LLT", "PT", "HLT", "HLGT", "SOC"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col] = df[col].replace("NAN", np.nan)

    # Converter grave
    if "GRAVE" in df.columns:
        df["GRAVE_BOOL"] = df["GRAVE"].astype(str).str.strip().str.upper().map({"SIM": True, "NAO": False, "NÃO": False})

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / "vigimed_reacoes.parquet"
    df.to_parquet(out, index=False)
    logger.info("  Salvo %s (%d linhas)", out, len(df))
    return df


def clean_all():
    """Executa limpeza de todos os datasets."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    for name, func in [
        ("medicamentos", clean_medicamentos),
        ("notificacoes", clean_notificacoes),
        ("vigimed_medicamentos", clean_vigimed_medicamentos),
        ("vigimed_reacoes", clean_vigimed_reacoes),
    ]:
        try:
            df = func()
            results[name] = len(df)
            print(f"  {name}: {len(df):,} linhas limpas")
        except Exception as e:
            logger.error("Erro na limpeza de %s: %s", name, e)
            raise

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_all()
