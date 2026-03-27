"""
Dashboard 1: Visão Executiva.
Painel resumido com KPIs, gráficos sintéticos e insights principais.
"""

from pathlib import Path

from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from src.dashboard.styles import COLORS, CHART_COLORS, CHART_CARD_STYLE, PLOTLY_TEMPLATE
from src.dashboard.components import kpi_card, insight_box, section_title
from src.analysis.eda import generate_summary_stats, generate_insight_bullets

PROCESSED_DIR = Path("data/processed")


def _safe_read(name: str) -> pd.DataFrame | None:
    path = PROCESSED_DIR / f"{name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def _format_number(n) -> str:
    """Formata número no padrão brasileiro."""
    if pd.isna(n):
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.1f}".replace(",", ".")
    return f"{int(n):,}".replace(",", ".")


def build_overview_layout() -> html.Div:
    """Constrói o layout completo do Dashboard 1."""

    # Carregar dados
    integrated = _safe_read("integrated_notifications")
    summary = _safe_read("summary_by_medication")
    monthly = _safe_read("agg_monthly_trend")
    by_class = _safe_read("agg_by_therapeutic_class")
    by_outcome = _safe_read("agg_by_outcome")
    med_reg = _safe_read("medicamentos")

    # --- KPIs ---
    stats = generate_summary_stats(integrated) if integrated is not None else {}

    total_med_reg = _format_number(len(med_reg)) if med_reg is not None else "N/A"
    total_notif = _format_number(stats.get("total_notificacoes", 0))
    taxa_grav = f"{stats.get('taxa_gravidade', 0)}%"
    top_med = stats.get("medicamento_mais_reportado", "N/A")
    top_med_n = _format_number(stats.get("medicamento_mais_reportado_n", 0))

    kpi_row = dbc.Row(
        [
            kpi_card("Medicamentos Registrados", total_med_reg, "Base ANVISA"),
            kpi_card("Notificacoes de Eventos Adversos", total_notif,
                     f"{stats.get('periodo_inicio', '?')}-{stats.get('periodo_fim', '?')}"),
            kpi_card("Taxa de Eventos Graves", taxa_grav, "Do total de notificacoes", color=COLORS["danger"]),
            kpi_card("Medicamento Mais Reportado", top_med[:25], f"{top_med_n} notificacoes"),
        ],
        className="mb-4",
    )

    # --- Gráfico 1: Tendência mensal ---
    fig_trend = go.Figure()
    if monthly is not None and len(monthly) > 0:
        fig_trend.add_trace(go.Scatter(
            x=monthly["MES_ANO"], y=monthly["TOTAL"],
            name="Total", line=dict(color=COLORS["secondary"], width=2),
            fill="tozeroy", fillcolor="rgba(46,134,193,0.1)",
        ))
        fig_trend.add_trace(go.Scatter(
            x=monthly["MES_ANO"], y=monthly["GRAVES"],
            name="Graves", line=dict(color=COLORS["danger"], width=2, dash="dot"),
        ))
    fig_trend.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        title=None, height=350, legend=dict(orientation="h", y=-0.15),
        xaxis_title=None, yaxis_title="Notificacoes",
    )
    # Show only every 12th tick for readability
    if monthly is not None and len(monthly) > 24:
        tickvals = monthly["MES_ANO"].iloc[::12].tolist()
        fig_trend.update_xaxes(tickvals=tickvals, tickangle=45)

    # --- Gráfico 2: Top 10 classes terapêuticas ---
    fig_classes = go.Figure()
    if by_class is not None and len(by_class) > 0:
        top10 = by_class.head(10).sort_values("TOTAL")
        fig_classes.add_trace(go.Bar(
            y=top10["ATC_NIVEL1_NOME"], x=top10["TOTAL"],
            orientation="h", marker_color=COLORS["secondary"],
            text=top10["TOTAL"].apply(lambda x: f"{x:,}".replace(",", ".")),
            textposition="outside",
        ))
    fig_classes.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        title=None, height=350, showlegend=False,
        xaxis_title="Notificacoes", yaxis_title=None,
        margin={"l": 200, "r": 60, "t": 20, "b": 40},
    )

    # --- Gráfico 3: Donut gravidade ---
    fig_donut = go.Figure()
    if integrated is not None and "GRAVE_BOOL" in integrated.columns:
        graves = integrated["GRAVE_BOOL"].sum()
        nao_graves = (~integrated["GRAVE_BOOL"].fillna(False)).sum()
        fig_donut.add_trace(go.Pie(
            labels=["Graves", "Nao Graves"],
            values=[graves, nao_graves],
            hole=0.6,
            marker_colors=[COLORS["danger"], COLORS["secondary"]],
            textinfo="percent+label",
            textfont_size=12,
        ))
        fig_donut.add_annotation(
            text=f"{_format_number(graves + nao_graves)}<br><span style='font-size:11px'>Total</span>",
            x=0.5, y=0.5, showarrow=False, font_size=18, font_color=COLORS["text"],
        )
    fig_donut.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        title=None, height=350, showlegend=False,
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
    )

    # --- Gráfico 4: Top 10 medicamentos por desfecho ---
    fig_outcome = go.Figure()
    if summary is not None and by_outcome is not None:
        top10_meds = summary.head(10)["NOME_MEDICAMENTO_WHODRUG"].tolist()
        if integrated is not None and "DESFECHO" in integrated.columns:
            df_top = integrated[integrated["NOME_MEDICAMENTO_WHODRUG"].isin(top10_meds)]
            pivot = df_top.groupby(["NOME_MEDICAMENTO_WHODRUG", "DESFECHO"]).size().reset_index(name="COUNT")
            # Top 4 desfechos
            top_outcomes = pivot.groupby("DESFECHO")["COUNT"].sum().nlargest(4).index.tolist()
            colors_map = {
                top_outcomes[0]: COLORS["success"] if len(top_outcomes) > 0 else COLORS["neutral"],
                top_outcomes[1]: COLORS["warning"] if len(top_outcomes) > 1 else COLORS["neutral"],
                top_outcomes[2]: COLORS["danger"] if len(top_outcomes) > 2 else COLORS["neutral"],
                top_outcomes[3]: COLORS["neutral"] if len(top_outcomes) > 3 else COLORS["neutral"],
            }
            for outcome in top_outcomes:
                subset = pivot[pivot["DESFECHO"] == outcome]
                fig_outcome.add_trace(go.Bar(
                    y=subset["NOME_MEDICAMENTO_WHODRUG"],
                    x=subset["COUNT"],
                    name=outcome[:30],
                    orientation="h",
                    marker_color=colors_map.get(outcome, COLORS["neutral"]),
                ))
    fig_outcome.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        title=None, height=350, barmode="stack",
        legend=dict(orientation="h", y=-0.2, font_size=10),
        xaxis_title="Notificacoes", yaxis_title=None,
        margin={"l": 200, "r": 20, "t": 20, "b": 60},
    )

    # --- Insights ---
    bullets = generate_insight_bullets(integrated) if integrated is not None else ["Dados nao disponiveis"]

    # --- Layout final ---
    return html.Div([
        kpi_row,

        dbc.Row([
            dbc.Col(html.Div([
                section_title("Evolucao mensal das notificacoes de eventos adversos"),
                dcc.Graph(figure=fig_trend, config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=7),
            dbc.Col(html.Div([
                section_title("Classes terapeuticas com mais notificacoes"),
                dcc.Graph(figure=fig_classes, config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=5),
        ], className="mb-3"),

        dbc.Row([
            dbc.Col(html.Div([
                section_title("Proporcao de eventos graves"),
                dcc.Graph(figure=fig_donut, config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=4),
            dbc.Col(html.Div([
                section_title("Top 10 medicamentos por desfecho"),
                dcc.Graph(figure=fig_outcome, config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=8),
        ], className="mb-3"),

        insight_box("Principais Insights", bullets),
    ])
