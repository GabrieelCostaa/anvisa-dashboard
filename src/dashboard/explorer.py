"""
Dashboard 2: Exploração Interativa.
Filtros dinâmicos, 7 visualizações e cross-filtering.
"""

from pathlib import Path

from dash import html, dcc, callback, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from src.dashboard.styles import (
    COLORS, CHART_COLORS, CHART_CARD_STYLE, PLOTLY_TEMPLATE, FILTER_LABEL_STYLE,
)
from src.dashboard.components import section_title

PROCESSED_DIR = Path("data/processed")

# Cache global dos dados
_DATA_CACHE = {}


def _load_data():
    """Carrega dados integrados (com cache)."""
    if "integrated" not in _DATA_CACHE:
        path = PROCESSED_DIR / "integrated_notifications.parquet"
        if path.exists():
            _DATA_CACHE["integrated"] = pd.read_parquet(path)
        else:
            _DATA_CACHE["integrated"] = pd.DataFrame()

    if "summary" not in _DATA_CACHE:
        path = PROCESSED_DIR / "summary_by_medication.parquet"
        if path.exists():
            _DATA_CACHE["summary"] = pd.read_parquet(path)
        else:
            _DATA_CACHE["summary"] = pd.DataFrame()

    if "reacoes" not in _DATA_CACHE:
        path = PROCESSED_DIR / "vigimed_reacoes.parquet"
        if path.exists():
            _DATA_CACHE["reacoes"] = pd.read_parquet(path)
        else:
            _DATA_CACHE["reacoes"] = pd.DataFrame()

    return _DATA_CACHE


def _get_filter_options(df: pd.DataFrame) -> dict:
    """Extrai opções únicas para os filtros."""
    options = {}

    if "ATC_NIVEL1_NOME" in df.columns:
        vals = df["ATC_NIVEL1_NOME"].dropna().unique()
        options["classes"] = sorted([{"label": v, "value": v} for v in vals], key=lambda x: x["label"])

    if "REGIAO" in df.columns:
        vals = df["REGIAO"].dropna().unique()
        options["regioes"] = sorted([{"label": v, "value": v} for v in vals], key=lambda x: x["label"])

    if "UF" in df.columns:
        vals = df["UF"].dropna().unique()
        options["ufs"] = sorted([{"label": v, "value": v} for v in vals], key=lambda x: x["label"])

    if "CATEGORIA_REGULATORIA_REG" in df.columns:
        vals = df["CATEGORIA_REGULATORIA_REG"].dropna().unique()
        options["categorias"] = sorted([{"label": v, "value": v} for v in vals], key=lambda x: x["label"])

    if "ANO_NOTIFICACAO" in df.columns:
        years = sorted(df["ANO_NOTIFICACAO"].dropna().unique())
        options["ano_min"] = int(years[0]) if len(years) > 0 else 2015
        options["ano_max"] = int(years[-1]) if len(years) > 0 else 2026

    return options


def build_explorer_layout() -> html.Div:
    """Constrói o layout do Dashboard 2."""
    data = _load_data()
    df = data.get("integrated", pd.DataFrame())
    opts = _get_filter_options(df)

    # --- Painel de filtros ---
    filters = dbc.Card([
        dbc.CardBody([
            html.H5("Filtros", className="mb-3", style={"fontWeight": "600", "color": COLORS["primary"]}),

            html.Label("Periodo", style=FILTER_LABEL_STYLE),
            dcc.RangeSlider(
                id="filter-year",
                min=opts.get("ano_min", 2015),
                max=opts.get("ano_max", 2026),
                value=[opts.get("ano_min", 2015), opts.get("ano_max", 2026)],
                marks={y: str(y) for y in range(opts.get("ano_min", 2015), opts.get("ano_max", 2026) + 1, 2)},
                step=1,
                className="mb-3",
            ),

            html.Label("Classe Terapeutica", style=FILTER_LABEL_STYLE),
            dcc.Dropdown(
                id="filter-class",
                options=opts.get("classes", []),
                multi=True,
                placeholder="Todas as classes...",
                className="mb-3",
            ),

            html.Label("Regiao", style=FILTER_LABEL_STYLE),
            dcc.Dropdown(
                id="filter-region",
                options=opts.get("regioes", []),
                multi=True,
                placeholder="Todas as regioes...",
                className="mb-3",
            ),

            html.Label("Gravidade", style=FILTER_LABEL_STYLE),
            dcc.Checklist(
                id="filter-severity",
                options=[
                    {"label": " Grave", "value": True},
                    {"label": " Nao Grave", "value": False},
                ],
                value=[True, False],
                className="mb-3",
                inputStyle={"marginRight": "6px"},
                labelStyle={"display": "block", "marginBottom": "4px"},
            ),

            html.Label("Categoria Regulatoria", style=FILTER_LABEL_STYLE),
            dcc.Dropdown(
                id="filter-category",
                options=opts.get("categorias", []),
                multi=True,
                placeholder="Todas as categorias...",
                className="mb-3",
            ),

            html.Label("UF", style=FILTER_LABEL_STYLE),
            dcc.Dropdown(
                id="filter-uf",
                options=opts.get("ufs", []),
                multi=True,
                placeholder="Todos os estados...",
                className="mb-3",
            ),

            html.Hr(),
            html.Label("Top N Medicamentos", style=FILTER_LABEL_STYLE),
            dcc.Slider(
                id="filter-topn",
                min=5, max=20, step=5, value=10,
                marks={5: "5", 10: "10", 15: "15", 20: "20"},
            ),
        ]),
    ], style={"border": f"1px solid {COLORS['border']}", "borderRadius": "12px"})

    # --- Área de gráficos ---
    charts = html.Div([
        # Row 1: Top N medicamentos + Tendência temporal
        dbc.Row([
            dbc.Col(html.Div([
                section_title("Medicamentos com mais notificacoes"),
                dcc.Graph(id="chart-top-meds", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=6),
            dbc.Col(html.Div([
                section_title("Tendencia temporal de notificacoes"),
                dcc.Dropdown(
                    id="trend-breakdown",
                    options=[
                        {"label": "Total", "value": "total"},
                        {"label": "Por Gravidade", "value": "gravidade"},
                        {"label": "Por Regiao", "value": "regiao"},
                    ],
                    value="total",
                    clearable=False,
                    style={"width": "200px", "marginBottom": "8px"},
                ),
                dcc.Graph(id="chart-trend", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=6),
        ], className="mb-3"),

        # Row 2: Scatter + Heatmap
        dbc.Row([
            dbc.Col(html.Div([
                section_title("Relacao entre volume de notificacoes e gravidade"),
                dcc.Graph(id="chart-scatter", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=6),
            dbc.Col(html.Div([
                section_title("Classe terapeutica vs. Sistema Organico (SOC)"),
                dcc.Graph(id="chart-heatmap", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=6),
        ], className="mb-3"),

        # Row 3: Treemap + Geografia + Desfecho
        dbc.Row([
            dbc.Col(html.Div([
                section_title("Hierarquia de reacoes adversas (MedDRA)"),
                dcc.Graph(id="chart-treemap", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=4),
            dbc.Col(html.Div([
                section_title("Distribuicao geografica por UF"),
                dcc.Graph(id="chart-geo", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=4),
            dbc.Col(html.Div([
                section_title("Distribuicao de desfechos"),
                dcc.Graph(id="chart-outcome", config={"displayModeBar": False}),
            ], style=CHART_CARD_STYLE), md=4),
        ], className="mb-3"),
    ])

    return html.Div([
        dbc.Row([
            dbc.Col(filters, md=3),
            dbc.Col(charts, md=9),
        ]),
    ])


def _apply_filters(df: pd.DataFrame, years, classes, regions, severity, categories, ufs) -> pd.DataFrame:
    """Aplica todos os filtros ao dataframe."""
    filtered = df.copy()

    if years and "ANO_NOTIFICACAO" in filtered.columns:
        filtered = filtered[
            (filtered["ANO_NOTIFICACAO"] >= years[0]) &
            (filtered["ANO_NOTIFICACAO"] <= years[1])
        ]

    if classes and "ATC_NIVEL1_NOME" in filtered.columns:
        filtered = filtered[filtered["ATC_NIVEL1_NOME"].isin(classes)]

    if regions and "REGIAO" in filtered.columns:
        filtered = filtered[filtered["REGIAO"].isin(regions)]

    if severity is not None and len(severity) < 2 and "GRAVE_BOOL" in filtered.columns:
        filtered = filtered[filtered["GRAVE_BOOL"].isin(severity)]

    if categories and "CATEGORIA_REGULATORIA_REG" in filtered.columns:
        filtered = filtered[filtered["CATEGORIA_REGULATORIA_REG"].isin(categories)]

    if ufs and "UF" in filtered.columns:
        filtered = filtered[filtered["UF"].isin(ufs)]

    return filtered


def register_callbacks(app):
    """Registra todos os callbacks do Dashboard 2."""

    @app.callback(
        Output("chart-top-meds", "figure"),
        Output("chart-trend", "figure"),
        Output("chart-scatter", "figure"),
        Output("chart-heatmap", "figure"),
        Output("chart-treemap", "figure"),
        Output("chart-geo", "figure"),
        Output("chart-outcome", "figure"),
        Input("filter-year", "value"),
        Input("filter-class", "value"),
        Input("filter-region", "value"),
        Input("filter-severity", "value"),
        Input("filter-category", "value"),
        Input("filter-uf", "value"),
        Input("filter-topn", "value"),
        Input("trend-breakdown", "value"),
    )
    def update_charts(years, classes, regions, severity, categories, ufs, topn, breakdown):
        data = _load_data()
        df = data.get("integrated", pd.DataFrame())
        reacoes = data.get("reacoes", pd.DataFrame())

        if df.empty:
            empty = go.Figure().update_layout(
                annotations=[{"text": "Dados nao disponiveis", "showarrow": False}]
            )
            return empty, empty, empty, empty, empty, empty, empty

        filtered = _apply_filters(df, years, classes, regions, severity, categories, ufs)

        if filtered.empty:
            empty = go.Figure().update_layout(
                annotations=[{"text": "Nenhum dado para os filtros selecionados", "showarrow": False}]
            )
            return empty, empty, empty, empty, empty, empty, empty

        # 1. Top N medicamentos
        top_meds = (
            filtered.groupby("NOME_MEDICAMENTO_WHODRUG")
            .agg(TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
                 GRAVES=("GRAVE_BOOL", "sum"))
            .reset_index()
            .nlargest(topn or 10, "TOTAL")
            .sort_values("TOTAL")
        )
        top_meds["NAO_GRAVES"] = top_meds["TOTAL"] - top_meds["GRAVES"]
        fig_top = go.Figure()
        fig_top.add_trace(go.Bar(
            y=top_meds["NOME_MEDICAMENTO_WHODRUG"], x=top_meds["NAO_GRAVES"],
            name="Nao Graves", orientation="h", marker_color=COLORS["secondary"],
        ))
        fig_top.add_trace(go.Bar(
            y=top_meds["NOME_MEDICAMENTO_WHODRUG"], x=top_meds["GRAVES"],
            name="Graves", orientation="h", marker_color=COLORS["danger"],
        ))
        fig_top.update_layout(
            **PLOTLY_TEMPLATE["layout"], barmode="stack",
            height=400, showlegend=True, legend=dict(orientation="h", y=-0.15),
            margin={"l": 200, "r": 20, "t": 10, "b": 50},
        )

        # 2. Tendência temporal
        fig_trend = go.Figure()
        if "MES_ANO" in filtered.columns:
            if breakdown == "gravidade":
                trend = filtered.groupby(["MES_ANO", "GRAVE_BOOL"]).size().reset_index(name="COUNT")
                for grave_val, label, color in [(True, "Graves", COLORS["danger"]), (False, "Nao Graves", COLORS["secondary"])]:
                    subset = trend[trend["GRAVE_BOOL"] == grave_val].sort_values("MES_ANO")
                    fig_trend.add_trace(go.Scatter(x=subset["MES_ANO"], y=subset["COUNT"], name=label, line=dict(color=color)))
            elif breakdown == "regiao" and "REGIAO" in filtered.columns:
                trend = filtered.groupby(["MES_ANO", "REGIAO"]).size().reset_index(name="COUNT")
                for i, regiao in enumerate(trend["REGIAO"].unique()):
                    subset = trend[trend["REGIAO"] == regiao].sort_values("MES_ANO")
                    fig_trend.add_trace(go.Scatter(x=subset["MES_ANO"], y=subset["COUNT"], name=regiao,
                                                    line=dict(color=CHART_COLORS[i % len(CHART_COLORS)])))
            else:
                trend = filtered.groupby("MES_ANO").size().reset_index(name="COUNT").sort_values("MES_ANO")
                fig_trend.add_trace(go.Scatter(x=trend["MES_ANO"], y=trend["COUNT"], name="Total",
                                                line=dict(color=COLORS["secondary"]), fill="tozeroy",
                                                fillcolor="rgba(46,134,193,0.1)"))
        fig_trend.update_layout(
            **PLOTLY_TEMPLATE["layout"], height=400,
            legend=dict(orientation="h", y=-0.2, font_size=10),
            margin={"l": 40, "r": 20, "t": 10, "b": 60},
        )
        if "MES_ANO" in filtered.columns:
            unique_months = filtered["MES_ANO"].dropna().sort_values().unique()
            if len(unique_months) > 24:
                fig_trend.update_xaxes(tickvals=unique_months[::12], tickangle=45)

        # 3. Scatter: volume vs gravidade por medicamento
        scatter_data = (
            filtered.groupby("NOME_MEDICAMENTO_WHODRUG")
            .agg(
                TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
                GRAVES=("GRAVE_BOOL", "sum"),
                CLASSE=("ATC_NIVEL1_NOME", "first"),
            )
            .reset_index()
        )
        scatter_data["TAXA_GRAVIDADE"] = (scatter_data["GRAVES"] / scatter_data["TOTAL"]).round(3)
        scatter_data = scatter_data[scatter_data["TOTAL"] >= 5]  # Filtrar ruído
        fig_scatter = px.scatter(
            scatter_data, x="TOTAL", y="TAXA_GRAVIDADE",
            color="CLASSE", size="TOTAL",
            hover_name="NOME_MEDICAMENTO_WHODRUG",
            color_discrete_sequence=CHART_COLORS,
            labels={"TOTAL": "Total Notificacoes", "TAXA_GRAVIDADE": "Taxa de Gravidade", "CLASSE": "Classe"},
        )
        fig_scatter.update_layout(
            **PLOTLY_TEMPLATE["layout"], height=400,
            legend=dict(font_size=9, orientation="h", y=-0.25),
            margin={"l": 50, "r": 20, "t": 10, "b": 70},
        )

        # 4. Heatmap: classe terapêutica vs SOC
        fig_heatmap = go.Figure()
        if not reacoes.empty and "SOC" in reacoes.columns:
            # Filtrar reações pelas notificações filtradas
            notif_ids = filtered["IDENTIFICACAO_NOTIFICACAO"].unique()
            reac_filt = reacoes[reacoes["IDENTIFICACAO_NOTIFICACAO"].isin(notif_ids)]

            # Juntar com classe do filtered
            if "ATC_NIVEL1_NOME" in filtered.columns:
                class_map = filtered[["IDENTIFICACAO_NOTIFICACAO", "ATC_NIVEL1_NOME"]].drop_duplicates()
                reac_with_class = reac_filt.merge(class_map, on="IDENTIFICACAO_NOTIFICACAO", how="inner")

                if not reac_with_class.empty:
                    pivot = reac_with_class.groupby(["ATC_NIVEL1_NOME", "SOC"]).size().reset_index(name="COUNT")
                    # Top 8 classes e top 8 SOCs
                    top_classes = pivot.groupby("ATC_NIVEL1_NOME")["COUNT"].sum().nlargest(8).index
                    top_socs = pivot.groupby("SOC")["COUNT"].sum().nlargest(8).index
                    pivot = pivot[pivot["ATC_NIVEL1_NOME"].isin(top_classes) & pivot["SOC"].isin(top_socs)]

                    if not pivot.empty:
                        matrix = pivot.pivot_table(index="ATC_NIVEL1_NOME", columns="SOC", values="COUNT", fill_value=0)
                        fig_heatmap = px.imshow(
                            matrix, color_continuous_scale="Blues",
                            labels={"color": "Notificacoes"},
                            aspect="auto",
                        )

        fig_heatmap.update_layout(
            **PLOTLY_TEMPLATE["layout"], height=400,
            margin={"l": 150, "r": 20, "t": 10, "b": 100},
            xaxis_tickangle=45,
        )

        # 5. Treemap: hierarquia MedDRA
        fig_treemap = go.Figure()
        if not reacoes.empty:
            notif_ids = filtered["IDENTIFICACAO_NOTIFICACAO"].unique()
            reac_filt = reacoes[reacoes["IDENTIFICACAO_NOTIFICACAO"].isin(notif_ids)]

            if "SOC" in reac_filt.columns and "HLGT" in reac_filt.columns:
                tree_data = (
                    reac_filt.groupby(["SOC", "HLGT"])
                    .size()
                    .reset_index(name="COUNT")
                    .nlargest(50, "COUNT")
                )
                if not tree_data.empty:
                    fig_treemap = px.treemap(
                        tree_data, path=["SOC", "HLGT"], values="COUNT",
                        color="COUNT", color_continuous_scale="Blues",
                    )

        fig_treemap.update_layout(
            **PLOTLY_TEMPLATE["layout"], height=400,
            margin={"l": 5, "r": 5, "t": 10, "b": 5},
        )

        # 6. Distribuição geográfica
        fig_geo = go.Figure()
        if "UF" in filtered.columns:
            geo_data = filtered.groupby("UF").agg(
                TOTAL=("IDENTIFICACAO_NOTIFICACAO", "nunique"),
            ).reset_index().sort_values("TOTAL")

            fig_geo = px.bar(
                geo_data, y="UF", x="TOTAL", orientation="h",
                color="TOTAL", color_continuous_scale="Blues",
                labels={"TOTAL": "Notificacoes", "UF": ""},
            )
            fig_geo.update_layout(
                **PLOTLY_TEMPLATE["layout"], height=400,
                showlegend=False, coloraxis_showscale=False,
                margin={"l": 40, "r": 20, "t": 10, "b": 30},
            )

        # 7. Desfecho (donut)
        fig_outcome = go.Figure()
        if "DESFECHO" in filtered.columns:
            outcome = filtered["DESFECHO"].value_counts().head(6)
            colors_out = [COLORS["success"], COLORS["warning"], COLORS["danger"],
                          COLORS["neutral"], COLORS["secondary"], COLORS["accent"]]
            fig_outcome.add_trace(go.Pie(
                labels=outcome.index, values=outcome.values,
                hole=0.5, marker_colors=colors_out[:len(outcome)],
                textinfo="percent+label", textfont_size=10,
            ))
        fig_outcome.update_layout(
            **PLOTLY_TEMPLATE["layout"], height=400,
            showlegend=False, margin={"l": 10, "r": 10, "t": 10, "b": 10},
        )

        return fig_top, fig_trend, fig_scatter, fig_heatmap, fig_treemap, fig_geo, fig_outcome
