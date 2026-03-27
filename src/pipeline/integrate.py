"""
Integração dos datasets da ANVISA.
Join entre VigiMed (notificações, medicamentos, reações) e medicamentos registrados.
"""

import re
import logging
from pathlib import Path

import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")

# Padrões de sais/formas farmacêuticas para remover na normalização
SALT_PATTERNS = [
    r"\bcloridrato\s+de\b", r"\bsulfato\s+de\b", r"\bacetato\s+de\b",
    r"\bmaleato\s+de\b", r"\bsuccinato\s+de\b", r"\bfumarato\s+de\b",
    r"\bcitrato\s+de\b", r"\bnitrato\s+de\b", r"\bfosfato\s+de\b",
    r"\bmesilato\s+de\b", r"\bbesilato\s+de\b", r"\btartarato\s+de\b",
    r"\bhydrochloride\b", r"\bsulfate\b", r"\bacetate\b",
    r"\bmaleate\b", r"\bsuccinate\b", r"\bfumarate\b",
    r"\bcitrate\b", r"\bnitrate\b", r"\bphosphate\b",
    r"\bmesylate\b", r"\bbesylate\b", r"\btartrate\b",
    r"\bsodium\b", r"\bpotassium\b", r"\bcalcium\b",
    r"\bsodio\b", r"\bpotassio\b", r"\bcalcio\b",
    r"\bdi-?\b", r"\btri-?\b",
]

SALT_REGEX = re.compile("|".join(SALT_PATTERNS), re.IGNORECASE)


def _normalize_ingredient(name: str) -> str:
    """Normaliza nome de princípio ativo para matching."""
    if pd.isna(name):
        return ""
    name = str(name).lower().strip()
    name = SALT_REGEX.sub("", name)
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _build_ingredient_lookup(med_df: pd.DataFrame) -> dict:
    """
    Constrói lookup de princípio ativo normalizado → info do medicamento registrado.
    Agrupa por princípio ativo para ter classe terapêutica e categoria.
    """
    if "PRINCIPIO_ATIVO" not in med_df.columns:
        logger.warning("Coluna PRINCIPIO_ATIVO nao encontrada nos medicamentos")
        return {}

    lookup = {}
    for _, row in med_df.iterrows():
        pa = row.get("PRINCIPIO_ATIVO")
        if pd.isna(pa):
            continue

        # Cada registro pode ter múltiplos princípios ativos separados por +
        for ingredient in str(pa).split("+"):
            normalized = _normalize_ingredient(ingredient)
            if normalized and normalized not in lookup:
                lookup[normalized] = {
                    "CLASSE_TERAPEUTICA": row.get("CLASSE_TERAPEUTICA"),
                    "CATEGORIA_REGULATORIA": row.get("CATEGORIA_REGULATORIA"),
                    "NOME_EMPRESA": row.get("NOME_EMPRESA"),
                    "SITUACAO_REGISTRO": row.get("SITUACAO_REGISTRO"),
                }

    logger.info("Lookup de principios ativos: %d entradas", len(lookup))
    return lookup


def _fuzzy_match_ingredient(name: str, lookup: dict, threshold: int = 80) -> dict | None:
    """Tenta match fuzzy de um princípio ativo contra o lookup."""
    normalized = _normalize_ingredient(name)
    if not normalized:
        return None

    # Match exato
    if normalized in lookup:
        return lookup[normalized]

    # Fuzzy match (só para ingredientes com >3 caracteres)
    if len(normalized) <= 3:
        return None

    best_score = 0
    best_match = None
    for key, info in lookup.items():
        score = fuzz.ratio(normalized, key)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = info

    return best_match


def integrate_vigimed() -> pd.DataFrame:
    """
    Integra as 3 tabelas do VigiMed (star schema).
    Retorna tabela de notificações enriquecida com medicamentos e reações.
    """
    logger.info("=== Integrando tabelas VigiMed ===")

    # Carregar
    notif = pd.read_parquet(PROCESSED_DIR / "vigimed_notificacoes.parquet")
    meds = pd.read_parquet(PROCESSED_DIR / "vigimed_medicamentos.parquet")
    reac = pd.read_parquet(PROCESSED_DIR / "vigimed_reacoes.parquet")

    logger.info("  Notificacoes: %d, Medicamentos: %d, Reacoes: %d", len(notif), len(meds), len(reac))

    # Filtrar apenas medicamentos suspeitos
    if "RELACAO_MEDICAMENTO_EVENTO" in meds.columns:
        meds_suspect = meds[meds["RELACAO_MEDICAMENTO_EVENTO"].str.contains("SUSPECT", case=False, na=False) |
                            meds["RELACAO_MEDICAMENTO_EVENTO"].str.contains("SUSPEITO", case=False, na=False)].copy()
        logger.info("  Medicamentos suspeitos: %d (de %d)", len(meds_suspect), len(meds))
    else:
        meds_suspect = meds.copy()

    # Agregar reações por notificação
    reac_agg = reac.groupby("IDENTIFICACAO_NOTIFICACAO").agg(
        N_REACOES=("PT", "count"),
        REACOES_PT=("PT", lambda x: "; ".join(x.dropna().unique()[:5])),
        SOC_LIST=("SOC", lambda x: "; ".join(x.dropna().unique()[:3])),
        TEM_GRAVE=("GRAVE_BOOL", lambda x: x.any() if x.notna().any() else None),
    ).reset_index()
    logger.info("  Reacoes agregadas: %d notificacoes", len(reac_agg))

    # Colunas que vêm dos medicamentos (evitar duplicatas no merge)
    med_cols = ["IDENTIFICACAO_NOTIFICACAO", "NOME_MEDICAMENTO_WHODRUG",
                "PRINCIPIOS_ATIVOS_WHODRUG", "CODIGO_ATC", "ATC_NIVEL1",
                "ATC_NIVEL2", "VIA_ADMINISTRACAO", "DOSE_VALOR"]
    med_cols = [c for c in med_cols if c in meds_suspect.columns]

    # Remover das notificações as colunas que virão dos medicamentos (exceto a key)
    overlap = set(notif.columns) & set(med_cols) - {"IDENTIFICACAO_NOTIFICACAO"}
    notif_clean = notif.drop(columns=list(overlap), errors="ignore")

    integrated = notif_clean.merge(
        meds_suspect[med_cols],
        on="IDENTIFICACAO_NOTIFICACAO",
        how="inner",
    )
    logger.info("  Apos join com medicamentos: %d linhas", len(integrated))

    # Join com reações agregadas
    integrated = integrated.merge(reac_agg, on="IDENTIFICACAO_NOTIFICACAO", how="left")
    logger.info("  Apos join com reacoes: %d linhas", len(integrated))

    return integrated


def enrich_with_registered(integrated: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece notificações com dados dos medicamentos registrados.
    Estratégia vetorizada: match exato por princípio ativo normalizado via merge.
    """
    logger.info("=== Enriquecendo com medicamentos registrados ===")
    med_reg = pd.read_parquet(PROCESSED_DIR / "medicamentos.parquet")

    if "PRINCIPIO_ATIVO" not in med_reg.columns or "PRINCIPIOS_ATIVOS_WHODRUG" not in integrated.columns:
        logger.warning("Colunas de principio ativo nao encontradas, pulando enriquecimento")
        integrated["CLASSE_TERAPEUTICA_REG"] = None
        integrated["CATEGORIA_REGULATORIA_REG"] = None
        return integrated

    # Construir tabela de lookup normalizada a partir dos medicamentos registrados
    reg_records = []
    for _, row in med_reg.iterrows():
        pa = row.get("PRINCIPIO_ATIVO")
        if pd.isna(pa):
            continue
        for ingredient in str(pa).split("+"):
            normalized = _normalize_ingredient(ingredient)
            if normalized:
                reg_records.append({
                    "PA_NORM": normalized,
                    "CLASSE_TERAPEUTICA_REG": row.get("CLASSE_TERAPEUTICA"),
                    "CATEGORIA_REGULATORIA_REG": row.get("CATEGORIA_REGULATORIA"),
                })

    if not reg_records:
        logger.warning("Nenhum principio ativo encontrado nos registrados")
        integrated["CLASSE_TERAPEUTICA_REG"] = None
        integrated["CATEGORIA_REGULATORIA_REG"] = None
        return integrated

    lookup_df = pd.DataFrame(reg_records).drop_duplicates(subset=["PA_NORM"], keep="first")
    logger.info("  Lookup: %d principios ativos normalizados", len(lookup_df))

    # Normalizar princípio ativo no integrated (vetorizado)
    integrated["PA_NORM"] = integrated["PRINCIPIOS_ATIVOS_WHODRUG"].apply(_normalize_ingredient)

    # Merge exato
    before = len(integrated)
    integrated = integrated.merge(lookup_df, on="PA_NORM", how="left")

    matched = integrated["CLASSE_TERAPEUTICA_REG"].notna().sum()
    pct = (matched / before * 100) if before > 0 else 0
    logger.info("  Match exato com registrados: %d/%d (%.1f%%)", matched, before, pct)

    # Limpar coluna auxiliar
    integrated = integrated.drop(columns=["PA_NORM"])

    return integrated


def build_summary(integrated: pd.DataFrame) -> pd.DataFrame:
    """Constrói tabela resumo: 1 linha por medicamento."""
    logger.info("=== Construindo tabela resumo por medicamento ===")

    summary = integrated.groupby("NOME_MEDICAMENTO_WHODRUG").agg(
        TOTAL_NOTIFICACOES=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
        TOTAL_GRAVES=("GRAVE_BOOL", lambda x: x.sum() if x.notna().any() else 0),
        MEDIA_IDADE=("IDADE_ANOS", "mean"),
        UFS=("UF", lambda x: "; ".join(x.dropna().unique()[:5])),
        PRINCIPIO_ATIVO=("PRINCIPIOS_ATIVOS_WHODRUG", "first"),
        CLASSE_TERAPEUTICA=("CLASSE_TERAPEUTICA_REG", "first"),
        CATEGORIA_REGULATORIA=("CATEGORIA_REGULATORIA_REG", "first"),
        ATC_NIVEL1=("ATC_NIVEL1", "first"),
        REACOES_COMUNS=("REACOES_PT", lambda x: "; ".join(x.dropna().unique()[:3])),
    ).reset_index()

    # Garantir tipos corretos para parquet
    summary["TOTAL_GRAVES"] = pd.to_numeric(summary["TOTAL_GRAVES"], errors="coerce").fillna(0).astype(int)
    summary["TAXA_GRAVIDADE"] = (summary["TOTAL_GRAVES"] / summary["TOTAL_NOTIFICACOES"]).round(3)
    summary = summary.sort_values("TOTAL_NOTIFICACOES", ascending=False)

    logger.info("  Resumo: %d medicamentos", len(summary))
    return summary


def integrate_all():
    """Executa toda a integração."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Integrar VigiMed
    integrated = integrate_vigimed()

    # Step 2: Enriquecer com registrados
    integrated = enrich_with_registered(integrated)

    # Step 3: Salvar integrado
    out_int = PROCESSED_DIR / "integrated_notifications.parquet"
    integrated.to_parquet(out_int, index=False)
    print(f"  Notificacoes integradas: {len(integrated):,} linhas -> {out_int}")

    # Step 4: Construir resumo
    summary = build_summary(integrated)
    out_sum = PROCESSED_DIR / "summary_by_medication.parquet"
    summary.to_parquet(out_sum, index=False)
    print(f"  Resumo por medicamento: {len(summary):,} linhas -> {out_sum}")

    return integrated, summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    integrate_all()
