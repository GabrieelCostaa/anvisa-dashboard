"""
Funções de análise exploratória para geração de insights.
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")


def load_data() -> dict[str, pd.DataFrame]:
    """Carrega todos os datasets processados disponíveis."""
    datasets = {}
    for f in PROCESSED_DIR.glob("*.parquet"):
        name = f.stem
        datasets[name] = pd.read_parquet(f)
        logger.info("  %s: %d linhas", name, len(datasets[name]))
    return datasets


def generate_summary_stats(df: pd.DataFrame) -> dict:
    """Gera estatísticas resumo do dataset integrado."""
    stats = {
        "total_notificacoes": df["IDENTIFICACAO_NOTIFICACAO"].nunique() if "IDENTIFICACAO_NOTIFICACAO" in df.columns else len(df),
        "total_medicamentos": df["NOME_MEDICAMENTO_WHODRUG"].nunique() if "NOME_MEDICAMENTO_WHODRUG" in df.columns else 0,
        "periodo_inicio": df["ANO_NOTIFICACAO"].min() if "ANO_NOTIFICACAO" in df.columns else None,
        "periodo_fim": df["ANO_NOTIFICACAO"].max() if "ANO_NOTIFICACAO" in df.columns else None,
    }

    if "GRAVE_BOOL" in df.columns:
        graves = df["GRAVE_BOOL"].sum()
        total = df["GRAVE_BOOL"].notna().sum()
        stats["total_graves"] = int(graves)
        stats["taxa_gravidade"] = round(graves / total * 100, 1) if total > 0 else 0

    if "IDADE_ANOS" in df.columns:
        stats["media_idade"] = round(df["IDADE_ANOS"].mean(), 1)
        stats["mediana_idade"] = round(df["IDADE_ANOS"].median(), 1)

    if "NOME_MEDICAMENTO_WHODRUG" in df.columns:
        top = df["NOME_MEDICAMENTO_WHODRUG"].value_counts().head(1)
        if len(top) > 0:
            stats["medicamento_mais_reportado"] = top.index[0]
            stats["medicamento_mais_reportado_n"] = int(top.values[0])

    return stats


def generate_insight_bullets(df: pd.DataFrame) -> list[str]:
    """Gera bullets de insights auto-gerados para o dashboard."""
    insights = []
    stats = generate_summary_stats(df)

    if "taxa_gravidade" in stats:
        insights.append(
            f"{stats['taxa_gravidade']}% das notificacoes sao classificadas como graves"
        )

    if "medicamento_mais_reportado" in stats:
        insights.append(
            f"O medicamento mais reportado e {stats['medicamento_mais_reportado']} "
            f"com {stats['medicamento_mais_reportado_n']:,} notificacoes"
        )

    if "REGIAO" in df.columns:
        top_regiao = df["REGIAO"].value_counts().head(1)
        if len(top_regiao) > 0:
            pct = round(top_regiao.values[0] / len(df) * 100, 1)
            insights.append(
                f"A regiao {top_regiao.index[0]} concentra {pct}% das notificacoes"
            )

    if "ATC_NIVEL1_NOME" in df.columns and "GRAVE_BOOL" in df.columns:
        grav_by_class = df.groupby("ATC_NIVEL1_NOME")["GRAVE_BOOL"].mean().sort_values(ascending=False)
        if len(grav_by_class) > 0:
            top_class = grav_by_class.index[0]
            top_rate = round(grav_by_class.values[0] * 100, 1)
            insights.append(
                f"{top_class} apresenta a maior taxa de gravidade ({top_rate}%)"
            )

    if "FAIXA_ETARIA" in df.columns:
        top_faixa = df["FAIXA_ETARIA"].value_counts().head(1)
        if len(top_faixa) > 0:
            insights.append(
                f"A faixa etaria {top_faixa.index[0]} anos e a mais afetada "
                f"({top_faixa.values[0]:,} notificacoes)"
            )

    return insights
