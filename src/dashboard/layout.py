"""
Layout principal do dashboard com navegação por tabs.
"""

from dash import html, dcc
import dash_bootstrap_components as dbc

from src.dashboard.styles import HEADER_STYLE, HEADER_TITLE_STYLE, HEADER_SUBTITLE_STYLE, COLORS
from src.dashboard.overview import build_overview_layout
from src.dashboard.explorer import build_explorer_layout


def build_layout() -> html.Div:
    """Constrói o layout completo da aplicação."""

    header = html.Div(
        [
            html.H1("ANVISA VigiMed", style=HEADER_TITLE_STYLE),
            html.P(
                "Dashboard de Farmacovigilancia — Analise de Eventos Adversos a Medicamentos no Brasil",
                style=HEADER_SUBTITLE_STYLE,
            ),
        ],
        style=HEADER_STYLE,
    )

    tabs = dbc.Tabs(
        [
            dbc.Tab(
                build_overview_layout(),
                label="Visao Executiva",
                tab_id="tab-overview",
                active_label_style={"color": COLORS["primary"], "fontWeight": "600"},
            ),
            dbc.Tab(
                build_explorer_layout(),
                label="Exploracao Interativa",
                tab_id="tab-explorer",
                active_label_style={"color": COLORS["primary"], "fontWeight": "600"},
            ),
        ],
        id="tabs",
        active_tab="tab-overview",
        className="mb-3",
    )

    footer = html.Footer(
        html.P(
            "Fonte: ANVISA — Dados Abertos (dados.anvisa.gov.br) | "
            "Projeto Final — Ciencia de Dados",
            style={
                "textAlign": "center",
                "color": COLORS["text_secondary"],
                "fontSize": "0.8rem",
                "padding": "16px",
                "marginTop": "24px",
            },
        )
    )

    return html.Div(
        [
            header,
            dbc.Container(
                [tabs, footer],
                fluid=True,
                style={"padding": "0 24px"},
            ),
        ],
        style={"backgroundColor": COLORS["bg"], "minHeight": "100vh"},
    )
